/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
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
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("MainframeReader")
@Description("Batch Source to read Mainframe fixed-length flat files")
public class MainframeSource extends BatchSource<LongWritable, Map<String, AbstractFieldValue>, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MainframeSource.class);

  public static final long DEFAULT_MAX_SPLIT_SIZE_IN_MB = 1;
  private static final long CONVERT_TO_BYTES = 1024 * 1024;
  private static final Pattern RTRIM = Pattern.compile("\\s+$");

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
    outputSchema = getOutputSchema(config.getCopyBookContents(), config.getFont());
    LOG.info("Output schema is: {}", outputSchema.toString());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    if (!Strings.isNullOrEmpty(config.keep)) {
      fieldsToKeep = new HashSet<>();
      for (String keepField : Splitter.on(",").trimResults().split(config.keep)) {
        fieldsToKeep.add(normalizeFieldName(keepField));
      }
    } else if (!Strings.isNullOrEmpty(config.drop)) {
      fieldsToDrop = new HashSet<>();
      for (String dropField : Splitter.on(",").trimResults().split(config.drop)) {
        fieldsToDrop.add(normalizeFieldName(dropField));
      }
    }
    if (config.maxSplitSize == null) {
      config.maxSplitSize = DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
    } else {
      config.maxSplitSize = config.maxSplitSize * CONVERT_TO_BYTES;
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
    CopybookInputFormat.setBinaryFilePath(job, config.binaryFilePath);
    // Set the input file path for the job
    CopybookInputFormat.setInputPaths(job, config.binaryFilePath);
    CopybookInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
    CopybookInputFormat.setCopybookInputformatCharset(job, config.getFont());
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(CopybookInputFormat.class,
                                                                                  job.getConfiguration())));
  }

  @Override
  public void transform(KeyValue<LongWritable, Map<String, AbstractFieldValue>> input,
                        Emitter<StructuredRecord> emitter)
    throws Exception {


    Map<String, AbstractFieldValue> values = Maps.newHashMap();
    for (Map.Entry<String, AbstractFieldValue> entry : input.getValue().entrySet()) {
      values.put(normalizeFieldName(entry.getKey()), entry.getValue());
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field field : outputSchema.getFields()) {
      String fieldName = field.getName();
      if (values.containsKey(fieldName)) {
        try {
          builder.set(fieldName, getFieldValue(values.get(fieldName)));
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
  public Schema getSchema(GetSchemaRequest request,
                          EndpointPluginContext pluginContext) {
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
        fields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(getFieldSchemaType(field.getType())))));
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
   * Get the field Schema.Type from the copybook data types
   *
   * @param type AbstractFiledValue type to be converted to CDAP Schema.Type
   * @return CDAP Schema.Type objects
   */
  private Schema.Type getFieldSchemaType(int type) {
    switch (type) {
      case 0:
        return Schema.Type.STRING;
      case 17:
        return Schema.Type.FLOAT;
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return Schema.Type.DOUBLE;
      case 25:
        return Schema.Type.INT;
      case 35:
      case 36:
      case 39:
        return Schema.Type.LONG;
      default:
        return Schema.Type.STRING;
    }
  }

  /**
   * Get the field values for the fields in the required format.
   * Date will be returned in the format - "yyyy-MM-dd".
   *
   * @param value AbstractFieldValue object to be converted in the JAVA primitive data types
   * @return data objects supported by CDAP
   * @throws ParseException
   */
  private Object getFieldValue(@Nullable AbstractFieldValue value) throws ParseException {
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

  /**
   * Config class for CopybookSource.
   */
  public static class MainframeSourceConfig extends ReferencePluginConfig {

    // Note: Charset is called font in jrecord terminology. It is the font for the file being used.
    // List of EBCDIC charsets cp037 (US), cp273 (Germany), cp500(German multinational), etc..
    // All the charset prefix with cp followed by CCSID (Coded Character set identifier),
    // List of all CCSID: https://www-01.ibm.com/software/globalization/ccsid/ccsid_registered.html
    // Relevant link: http://www.easymarketplace.de/codepages.php
    private static Map<String, String> charsetToFontLookup = new ImmutableMap.Builder<String, String>()
                                                                        .put("EBCDIC-US", "cp037")
                                                                        .put("EBCDIC-Arabic", "cp420")
                                                                        .put("EBCDIC-Denmark, Norway", "cp277")
                                                                        .put("EBCDIC-France", "cp297")
                                                                        .put("EBCDIC-Germany", "cp237")
                                                                        .put("EBCDIC-Greece", "cp875")
                                                                        .put("EBCDIC-International", "cp500")
                                                                        .put("EBCDIC-Italy", "cp280")
                                                                        .put("EBCDIC-Russia", "cp410")
                                                                        .put("EBCDIC-Spain", "cp283")
                                                                        .put("EBCDIC-Thailand", "cp838")
                                                                        .put("EBCDIC-Turkey", "cp322").build();

    @VisibleForTesting
    static final String DEFAULT_FONT = "cp037";

    @Description("Complete path of the .bin to be read; for example: 'hdfs://10.222.41.31:9000/test/DTAR020_FB.bin' " +
      "or 'file:///home/cdap/DTAR020_FB.bin'.\n " +
      "This will be a fixed-length binary format file that matches the copybook.\n" +
      "(This is done to accept files present on a remote HDFS location.)")
    @Macro
    private String binaryFilePath;

    @VisibleForTesting
    @Description("Contents of the COBOL copybook file which will contain the data structure. For example: \n" +
      "000100*                                                                         \n" +
      "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
      "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
      "000400*                                                                         \n" +
      "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
      "000600*                                                                         \n" +
      "000700*   RECORD LENGTH IS 27.                                                  \n" +
      "000800*                                                                         \n" +
      "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
      "001000            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
      "001100            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
      "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3. ")
    String copybookContents;

    @Nullable
    @Description("Comma-separated list of fields to drop. For example: 'field1,field2,field3'. " +
      "If both fields to drop and fields to keep are given, fields to keep prevails.")
    private String drop;

    @Nullable
    @Description("Comma-separated list of fields to keep. For example: 'field1,field2,field3'. " +
      "If both fields to drop and fields to keep are given, fields to keep prevails.")
    private String keep;

    @Nullable
    @Description("Maximum split-size(MB) for each mapper in the MapReduce Job. Defaults to 1MB.")
    @Macro
    private Long maxSplitSize;

    @Nullable
    @Description("Code page to use - an alternative notation to the Charset. For example, cp037 or cp322. " +
      "If this is configured, it overrides the charset property.")
    @VisibleForTesting
    String codepage;

    @Nullable
    @Description("Charset used to read the data. Available Charset: EBCDIC-US (cp307), EBCDIC-Germany (cp237), " +
                   "EBCDIC-Arabic (cp420), EBCDIC-Denmark, Norway (cp277), EBCDIC-France (cp297),\n" +
                   "EBCDIC-Greece (cp875), EBCDIC-International (cp500), EBCDOC-Italy (cp280), EBCDIC-Russia (cp410)," +
                   " EBCDIC-Spain (cp383), EBCDIC-Thailand (cp838), EBCDIC-Turkey (cp322).")
    @VisibleForTesting
    String charset;

    @Nullable
    @Description("List of placeholder strings and their replacements, for example, 'a=b,x=y'. All occurrences of " +
      "placeholders are replaced prior to loading the copybook. If after replacements a line of the copybook " +
      "exceeds 72 characters, the copybook will not be accepted and this will result in an error.")
    @VisibleForTesting
    String replacements;

    public String getFont() {
      if (codepage != null && !codepage.isEmpty()) {
        return codepage;
      }
      if (charset != null && !charset.isEmpty()) {
        String font = charsetToFontLookup.get(charset);
        if (font != null && !font.isEmpty()) {
          return font;
        }
      }
      return DEFAULT_FONT;
    }

    public MainframeSourceConfig() {
      super(String.format("CopybookReader"));
      this.maxSplitSize = DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
    }

    @VisibleForTesting
    public Long getMaxSplitSize() {
      return maxSplitSize;
    }

    @Nullable
    public Map<String, String> getReplacements() {
      if (replacements == null) {
        return null;
      }
      Map<String, String> result = new HashMap<>();
      for (String pair : replacements.split(",")) {
        String[] subs = pair.split("=");
        if (subs.length != 2) {
          throw new IllegalArgumentException("Replacements must be of the form 'a=x,b=y,...' but is " + replacements);
        }
        result.put(subs[0].trim(), subs[1].trim());
      }
      return result.isEmpty() ? null : result;
    }

    public String getCopyBookContents() {
      String copybook = copybookContents;
      Map<String, String> replacementMap = getReplacements();
      if (replacementMap != null) {
        for (Map.Entry<String, String> entry : replacementMap.entrySet()) {
          copybook = copybook.replaceAll(entry.getKey(), entry.getValue());
        }
      }
      int lineNo = 0;
      for (String line : copybook.split("\\r?\\n")) {
        ++lineNo;
        for (int i = line.length() - 1; i > 0; i--) {
          String trimmed = RTRIM.matcher(line).replaceAll("");
          if (trimmed.length() > 72) {
            String message = String.format("Copybook line %d exceeds the maximum length of 72 characters. " +
                                             "This may be caused by replacement strings that exceed the length " +
                                             "of the placeholders they substitute. Please check your copybook and " +
                                             "your replacements. ", lineNo);
            throw new IllegalArgumentException(message);
          }
        }
      }
      return copybook;
    }
  }
}
