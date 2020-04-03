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
package io.cdap.plugin.batch.mainframe.reader;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import net.sf.JRecord.Common.AbstractFieldValue;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Batch source to poll fixed-length flat files that can be parsed using a COBOL copybook.
 * <p>
 * The plugin will accept the copybook contents in a textbox and a binary data file.
 * It produces structured records based on the schema as defined either by the copybook contents or the user.
 * <p>
 * For this first implementation, it will only accept binary fixed-length flat files without any nesting.
 */
public class MainframeSource extends BatchSource<LongWritable, Map<String, AbstractFieldValue>, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MainframeSource.class);

  private final MainframeSourceConfig config;
  private Schema outputSchema;
  private Set<String> fieldsToKeep;
  private Set<String> fieldsToDrop;

  public MainframeSource(MainframeSourceConfig mainframeSourceConfig) {
    this.config = mainframeSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector);
    failureCollector.getOrThrowException();

    outputSchema = getOutputSchema(config.getCopyBookContents(), config.getFont());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    if (!Strings.isNullOrEmpty(config.getKeep())) {
      fieldsToKeep = new HashSet<>();
      Splitter.on(",").trimResults().split(config.getKeep())
        .forEach(keepField -> fieldsToKeep.add(normalizeFieldName(keepField)));
    } else if (!Strings.isNullOrEmpty(config.getDrop())) {
      fieldsToDrop = new HashSet<>();
      Splitter.on(",").trimResults().split(config.getDrop())
        .forEach(dropField -> fieldsToDrop.add(normalizeFieldName(dropField)));
    }
    outputSchema = getOutputSchema(config.getCopyBookContents(), config.getFont());
  }

  private String normalizeFieldName(String fieldName) {
    return fieldName.replace("-", "_");
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    Job job = JobUtils.createInstance();
    CopybookInputFormat.setCopybookInputformatCblContents(job, config.getCopyBookContents());
    CopybookInputFormat.setBinaryFilePath(job, config.getBinaryFilePath());
    // Set the input file path for the job
    CopybookInputFormat.setInputPaths(job, config.getBinaryFilePath());
    CopybookInputFormat.setMaxInputSplitSize(job, config.getMaxSplitSize());
    CopybookInputFormat.setCopybookInputformatCharset(job, config.getFont());
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CopybookInputFormat.class,
                                                                                  job.getConfiguration())));
  }

  @Override
  public void transform(KeyValue<LongWritable, Map<String, AbstractFieldValue>> input,
                        Emitter<StructuredRecord> emitter)
    throws Exception {

    Map<String, AbstractFieldValue> values = input.getValue();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (values.containsKey(fieldName)) {
        try {
          builder.set(fieldName, values.get(fieldName).asString());
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format(
            "Unable to extract value for field '%s' in record at offset %d: %s",
            fieldName, input.getKey().get(), e.getMessage()));
        }
      }
    }
    emitter.emit(builder.build());
  }

  class GetSchemaRequest {
    public String copybookContents;
  }

  @Path("outputSchema")
  public Schema getSchema(GetSchemaRequest request) {
      return getOutputSchema(request.copybookContents, MainframeSourceConfig.DEFAULT_FONT);
  }

  /**
   * Get the output schema from the COBOL copybook contents specified by the user.
   *
   * @return outputSchema
   */
  private Schema getOutputSchema(String copybookContents, String font) {

    InputStream inputStream;
    ExternalRecord externalRecord;
    List<Schema.Field> fields = Lists.newArrayList();
    try {
      inputStream = IOUtils.toInputStream(copybookContents, "UTF-8");
      BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      externalRecord = CopybookIOUtils.getExternalRecord(bufferedInputStream, font);
      String fieldName;
      for (ExternalField field : externalRecord.getRecordFields()) {
        fieldName = normalizeFieldName(field.getName());
        if (fieldsToKeep != null && !fieldsToKeep.contains(fieldName)) {
            continue;
        }
        if (fieldsToDrop != null && fieldsToDrop.contains(fieldName)) {
          continue;
        }
        fields.add(Schema.Field.of(fieldName, Schema.nullableOf(
          Schema.of(Schema.Type.STRING))));
      }
      return Schema.recordOf("record", fields);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while creating input stream for COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    } catch (RecordException e) {
      throw new IllegalArgumentException("Exception while creating record from COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    }
  }

  /**
   * Get the field values for the fields in the required format.
   * Date will be returned in the format - "yyyy-MM-dd".
   *
   * @param value AbstractFieldValue object to be converted in the JAVA primitive data types
   * @return data objects supported by CDAP
   */
  private Object getFieldValue(@Nullable AbstractFieldValue value) {
    if (value == null) {
      return null;
    }
    int type = value.getFieldDetail().getType();
    switch (type) {
      case 0:
        return value.asString();
      case 17:
        return value.asFloat();
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return value.asDouble();
      case 25:
        return value.asInt();
      case 35:
      case 36:
      case 39:
        return value.asLong();
      default:
        return value.asString();
    }
  }

}
