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

package io.cdap.plugin.mainframe.schema;

import com.legstar.cob2xsd.Cob2XsdConfig;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.mainframe.common.AvroConverter;
import io.cdap.plugin.mainframe.common.StreamCharSource;
import io.cdap.plugin.mainframe.reader.CopybookReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * This class <code>CopybookToSchema</code> transforms COBOL copybook to Avro compatible schema.
 */
public final class CopybookSchema {
  private String copybook;
  private String codeFormat;

  public CopybookSchema(String copybook, String codeFormat) {
    this.copybook = copybook;
    this.codeFormat = codeFormat;
  }

  public String getCodeFormat() {
    return Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name();
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

  /**
   * Generates a <code>Schema</code> based on the copybook provided.
   * @return
   * @throws IOException
   */
  public Schema getSchema() throws IOException {
    CopybookReader copybookReader;
    try {
      copybookReader = getCopybookReader();
    } catch (Exception ex) {
      throw new IOException(String.format("Error while reading copybook: '%s'", ex.getMessage()));
    }

    try {
      return getOutputSchemaAndValidate(copybookReader);
    } catch (Exception ex) {
      throw new IOException(String.format("Error while generating schema from the copybook: '%s'", ex.getMessage()));
    }
  }
}
