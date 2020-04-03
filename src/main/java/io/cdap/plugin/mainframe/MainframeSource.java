/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Batch source to poll fixed-length flat files that can be parsed using a COBOL copybook.
 * <p>
 * The plugin will accept the copybook contents in a textbox and a binary data file.
 * It produces structured records based on the schema as defined either by the copybook contents or the user.
 * <p>
 * For this first implementation, it will only accept binary fixed-length flat files without any nesting.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Mainframe")
@Description("Read EBCDIC mainframe fixed length and variable length record files.")
public class MainframeSource extends BatchSource<LongWritable, MainframeRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MainframeSource.class);
  private final Config config;
  private Schema outputSchema;

  public MainframeSource(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    CopybookToSchema copybookToSchema = new CopybookToSchema(config.getCopyBookContents(), config.getFont());
    try {
      outputSchema = copybookToSchema.getSchema(true);
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    CopybookToSchema copybookToSchema = new CopybookToSchema(config.getCopyBookContents(), config.getFont());
    outputSchema = copybookToSchema.getSchema(true);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    Job job = JobUtils.createInstance();
    MainframeInputFormat.setInputPaths(job, config.getBinaryFilePath());
    MainframeInputFormat.setMaxInputSplitSize(job, config.getMaxSplitSize());
    MainframeInputFormat.setFont(job, config.getFont());
    MainframeInputFormat.setCopybookContent(job, config.getCopyBookContents());
    MainframeInputFormat.setBinaryFilePath(job, config.getBinaryFilePath());
    context.setInput(Input.of(
      config.referenceName, new SourceInputFormatProvider(
        MainframeInputFormat.class, job.getConfiguration()
      ))
    );
  }

  @Override
  public void transform(KeyValue<LongWritable, MainframeRecord> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    MainframeRecord record = input.getValue();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (record.get(fieldName) != null) {
        try {
          builder.set(fieldName, record.get(fieldName).getValue().asString());
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format(
            "Unable to extract value for field '%s' in record at offset %d: %s",
            fieldName, input.getKey().get(), e.getMessage()));
        }
      }
    }
    emitter.emit(builder.build());
  }
}
