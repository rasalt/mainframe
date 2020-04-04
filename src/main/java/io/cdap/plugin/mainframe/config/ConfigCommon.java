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

package io.cdap.plugin.mainframe.config;

import com.legstar.cob2xsd.Cob2XsdConfig;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.mainframe.common.AvroConverter;
import io.cdap.plugin.mainframe.common.StreamCharSource;
import io.cdap.plugin.mainframe.reader.CopybookReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Common config.
 */
public class ConfigCommon extends PluginConfig {
  public static final String PROPERTY_COPYBOOK = "copybook";
  public static final String PROPERTY_CHARSET = "charset";
  public static final String PROPERTY_CODEFORMAT = "codeformat";
  public static final String PROPERTY_RDW = "rdw";

  @Name(PROPERTY_COPYBOOK)
  @Description("COBOL Copybook")
  @Macro
  private final String copybook;

  @Name(PROPERTY_CHARSET)
  @Description("Charset used to read the data. Default Charset is 'IBM01140'.")
  @Nullable
  @Macro
  private final String charset;

  @Name(PROPERTY_CODEFORMAT)
  @Description("Code format in copybook")
  @Nullable
  @Macro
  private final String codeFormat;

  @Name(PROPERTY_RDW)
  @Description("Records start with Record Descriptor Word")
  @Nullable
  @Macro
  private final Boolean rdw;

  public ConfigCommon(String copybook, @Nullable String charset,
                      @Nullable String codeFormat, @Nullable Boolean rdw) {
    super();
    this.copybook = copybook;
    this.charset = charset;
    this.codeFormat = codeFormat;
    this.rdw = rdw;
  }

  public String getCopybook() {
    return copybook;
  }

  public String getCodeFormat() {
    return codeFormat == null ? Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name() : codeFormat;
  }

  public String getCharset() {
    return charset == null ? "IBM01140" : charset;
  }

  public boolean hasRDW() {
    return rdw == null ? true : rdw;
  }

  public byte[] getCopybookBytes() {
    return copybook.getBytes(StandardCharsets.UTF_8);
  }

  public CopybookReader getCopybookReader() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, getCodeFormat());
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(getCopybookBytes()));
    return new CopybookReader(streamCharSource, properties);
  }

  public Schema getOutputSchemaAndValidate(CopybookReader copybookReader) {
    org.apache.avro.Schema avroSchema = copybookReader.getSchema();
    return AvroConverter.fromAvroSchema(avroSchema);
  }

  public Schema getOutputSchemaAndValidate(FailureCollector failureCollector, Schema inputSchema) {
    if (!Charset.isSupported(getCharset())) {
      failureCollector.addFailure(String.format("The charset name '%s' is not supported by your java environment.",
                                                getCharset()),
                                  "Make sure you have lib/charsets.jar in your jre.")
        .withConfigProperty(PROPERTY_CHARSET);
      // if above failed, we cannot proceed to copybook parsing.
      throw failureCollector.getOrThrowException();
    }

    CopybookReader copybookReader;
    try {
      copybookReader = getCopybookReader();
    } catch (Exception ex) {
      failureCollector.addFailure(String.format("Error while reading copybook: '%s'", ex.getMessage()),
                                  "Please make sure it has correct format")
        .withConfigProperty(PROPERTY_COPYBOOK)
        .withStacktrace(ex.getStackTrace());
      throw failureCollector.getOrThrowException();
    }

    try {
      return getOutputSchemaAndValidate(copybookReader);
    } catch (Exception ex) {
      failureCollector.addFailure(String.format("Error while generating schema from the copybook: '%s'",
                                                ex.getMessage()), null)
        .withConfigProperty(PROPERTY_COPYBOOK)
        .withStacktrace(ex.getStackTrace());
      throw failureCollector.getOrThrowException();
    }
  }
}
