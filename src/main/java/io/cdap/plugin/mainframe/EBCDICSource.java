/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.mainframe;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.mainframe.config.SourceConfig;
import io.cdap.plugin.mainframe.format.MainframeInputFormat;
import io.cdap.plugin.mainframe.format.MainframeRecord;
import io.cdap.plugin.mainframe.schema.CopybookSchema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * {@link EBCDICSource} is a EBCDIC mainframe reader.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Mainframe")
@Description("EBCDIC mainframe reader for fixed blocked and variable blocked files.")
public class EBCDICSource extends BatchSource<LongWritable, MainframeRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(EBCDICSource.class);
  private final SourceConfig config;
  private Schema outputSchema;
  private static final Schema ERROR_SCHEMA =
    Schema.recordOf("error", Schema.Field.of("error", Schema.of(Schema.Type.STRING)));

  public EBCDICSource(SourceConfig config) {
    this.config = config;
  }

  /**
   * Configures this plugin.
   * @param configurer handle for configuring the plugin.
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);

    FailureCollector failureCollector = configurer.getStageConfigurer().getFailureCollector();
    outputSchema = config.getOutputSchemaAndValidate(failureCollector);

    if (config.getFilepath().trim().isEmpty()) {
      failureCollector.addFailure(String.format("The path to file is not specified."),
                                  "Make sure you provide a valid path for a file or directory")
        .withConfigProperty(config.PROPERTY_FILEPATH);
      throw failureCollector.getOrThrowException();
    }
    failureCollector.getOrThrowException();

    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  /**
   * Prepares the pipeline before pipeline starts executing.
   *
   * @param context handler for batch context.
   * @throws IOException thrown if there is any issue validating or configuring this plugin.
   */
  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    // Prepare to pass all the arguments to all input formats.
    Job job = JobUtils.createInstance();
    MainframeInputFormat.setCopybook(job, config.getCopybook());
    MainframeInputFormat.setCharset(job, config.getCharset());
    MainframeInputFormat.setInputPaths(job, config.getFilepath());
    MainframeInputFormat.setCodeFormat(job, config.getCodeFormat());
    MainframeInputFormat.setBinaryFilePath(job, config.getFilepath());
    context.setInput(Input.of(
      config.referenceName, new SourceInputFormatProvider(
        MainframeInputFormat.class, job.getConfiguration()
      ))
    );

    // Generate schema from copybook and emit lineage.
    // TODO: Lineage for tested field needs some work.
    CopybookSchema copybookToSchema = new CopybookSchema(config.getCopybook(), config.getCodeFormat());
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    outputSchema = copybookToSchema.getSchema();
    lineageRecorder.createExternalDataset(outputSchema);

    StringBuilder sb = new StringBuilder("Reading mainframe data from '");
    sb.append(config.getFilepath()).append("', based on the provided copybook. ");
    lineageRecorder.recordRead("Read", sb.toString(), Preconditions.checkNotNull(outputSchema.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));
  }

  /**
   * Initialises this plugin.
   *
   * @param context execution context.
   * @throws Exception thrown if there is any issue with generating copybook.
   */
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  /**
   * Handles the record read from the source.
   *
   * @param input record that was read by the <code>MainframeRecoder</code>.
   * @param emitter emits either error or actual read.
   * @throws Exception thrown if there is issuing reading records.
   */
  @Override
  public void transform(KeyValue<LongWritable, MainframeRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    MainframeRecord record = input.getValue();
    if (record.hasError()) {
      StructuredRecord.Builder builder = StructuredRecord.builder(ERROR_SCHEMA);
      builder.set("error", record.getErrorMessage());
      emitter.emitError(new InvalidEntry<>(1, record.getErrorMessage(), builder.build()));
    } else {
      emitter.emit(record.getRecord());
    }
  }
}
