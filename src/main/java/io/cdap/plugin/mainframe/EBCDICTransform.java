/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

import com.legstar.avro.cob2avro.io.AbstractZosDatumReader;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.mainframe.common.AvroConverter;
import io.cdap.plugin.mainframe.common.StreamByteSource;
import io.cdap.plugin.mainframe.config.TransformConfig;
import io.cdap.plugin.mainframe.reader.CopybookReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

/**
 * {@link EBCDICTransform} converts a incoming EBCDIC formatted data into a record.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Mainframe")
@Description("Convert EBCDIC formatted data into a record.")
public class EBCDICTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(EBCDICTransform.class);
  private final TransformConfig config;
  private CopybookReader copybookReader;
  private Schema schema;

  public EBCDICTransform(TransformConfig config) {
    this.config = config;
  }
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    this.copybookReader = config.getCopybookReader();
    this.schema = config.getOutputSchema(copybookReader);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    Schema outputSchema = config.getOutputSchemaAndValidate(failureCollector);
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    byte[] body = input.get(config.getFieldName());
    StreamByteSource source = new StreamByteSource(new ByteArrayInputStream(body), body.length);
    try (AbstractZosDatumReader<GenericRecord> reader = copybookReader.createRecordReader(source, config.getCharset(),
                                                                                          config.hasRDW())) {
      for (GenericRecord record : reader) {
        LOG.info(StructuredRecordStringConverter.toJsonString(AvroConverter.fromAvroRecord(record, schema)));
        emitter.emit(AvroConverter.fromAvroRecord(record, schema));
      }
    }
  }
}
