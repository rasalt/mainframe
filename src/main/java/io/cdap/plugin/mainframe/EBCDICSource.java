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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.mainframe.config.SourceConfig;
import io.cdap.plugin.mainframe.format.MainframeInputFormat;
import io.cdap.plugin.mainframe.schema.CopybookSchema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link EBCDICSource} is a EBCDIC mainframe reader.
 *
 * This class <code>EBCDICSource</code> supports reading of fixed length and variable block mainframe file formats.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Mainframe")
@Description("Read EBCDIC mainframe fixed length and variable length record files.")
public class EBCDICSource extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(EBCDICSource.class);
  private final SourceConfig config;
  private Schema outputSchema;

  public EBCDICSource(SourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Schema outputSchema = config.getOutputSchemaAndValidate(failureCollector, inputSchema);

    if (config.getFilepath().trim().isEmpty()) {
      failureCollector.addFailure(String.format("The path to file is not specified."),
                                  "Make sure you provide a valid path for a file or directory")
        .withConfigProperty(config.PROPERTY_FILEPATH);
      throw failureCollector.getOrThrowException();
    }
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    CopybookSchema copybookToSchema = new CopybookSchema(config.getCopybook(), config.getCodeFormat());
    outputSchema = copybookToSchema.getSchema();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
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
  }
}
