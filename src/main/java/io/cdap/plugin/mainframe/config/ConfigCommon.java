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
 * This class <code>ConfigCommon</code> is the base class for all mainframe plugins.
 *
 * There are four properties that are common across all the mainframe plugins
 * <ul>
 *   <li>
 *     Copybook
 *     <p>
 *       A COBOL copybook is a selection of COBOL code that defines the data structures. Generally,
 *       if a particular data structure is used in many programs (like reader and writer), then
 *       instead of writing the same data structure again, they use copybooks.
 *     </p>
 *   </li>
 *   <li>
 *     Charset
 *     <p>
 *       The list of available character sets is determined by the Java Runtime Environment (JRE).
 *       Most of the time the JRE will have the character set you need. However, if you are using EBCDIC,
 *       the default character sets that come with the JRE do not include the EBCDIC character sets.
 *       There is no single character set for EBCDIC. Rather, there are EBCDIC character sets for different locales.
 *       For example, the English EBCDIC encoding is called IBM01140 or CP037. When referencing the links below
 *       that describe the character sets, the EBCDIC character sets generally included in those are identified
 *       as IBM, but there are many IBM character sets on the list that are not actually EBCDIC.
 *
 *       If your character set is not present, it's likely part of the extended characters sets that are not
 *       automatically installed into your Java Runtime Environment (JRE). These links list the supported character
 *       sets for JRE 5 or JRE 6. To install the extended character set, get the charsets.jar file, which is an option
 *       in the Java installation, and place it in the lib directory of your JRE. See your system administrator if
 *       you need help with this.
 *
 *       If the character set is not present in any of the lists, then it is invalid and needs to be changed to a
 *       value that is on the list.
 *     </p>
 *   </li>
 *   <li>
 *     Code Format
 *     <p>
 *       Code format defines where the copybook is Fixed width (legacy) that has seq nos, etc or a free form.
 *       Free forms are newer versions that are more lenient.
 *     </p>
 *   </li>
 *   <li>
 *     Record Descriptor Word (RDW)
 *     <p>
 *       RDW describes if the data file to be processed is Fixed or Variable. If RDW is true, then we consider the data
 *       file to either variable or variable blocked else, it's considered as fixed or fixed blocked.
 *       Traditional z/OS data sets have one of four record formats, as follows:
 *       <ul>
 *         <li>
 *           F (Fixed) - Fixed means that one physical block on disk is one logical record and all the blocks
 *           and records are the same size. This format is seldom used.
 *         </li>
 *         <li>
 *           FB (Fixed Blocked) - This format designation means that several logical records are combined
 *           into one physical block. This format can provide efficient space utilization and operation.
 *           This format is commonly used for fixed-length records.
 *         </li>
 *         <li>
 *           V (Variable) - This format has one logical record as one physical block. A variable-length logical
 *           record consists of a record descriptor word (RDW) followed by the data. The record descriptor word
 *           is a 4-byte field describing the record. The first 2 bytes contain the length of the logical record
 *           (including the 4-byte RDW). The length can be from 4 to 32,760 bytes. All bits of the third and
 *           fourth bytes must be 0, because other values are used for spanned records. This format is seldom used.
 *         </li>
 *         <li>
 *           VB (Variable Blocked) - This format places several variable-length logical records (each with an RDW)
 *           in one physical block. The software must place an additional Block Descriptor Word (BDW) at the beginning
 *           of the block, containing the total length of the block.
 *         </li>
 *       </ul>
 *     </p>
 *   </li>
 * </ul>
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

  /**
   * @return a string representation of copybook.
   */
  public String getCopybook() {
    return copybook;
  }

  /**
   * @return a string representing whether the cobol copybook should be considered fixed or free format.
   */
  public String getCodeFormat() {
    return codeFormat == null ? Cob2XsdConfig.CodeFormat.FIXED_FORMAT.name() : codeFormat;
  }

  /**
   * @return Encoding to be used for the parsing the binary file.
   */
  public String getCharset() {
    return charset == null ? "IBM01140" : charset;
  }

  /**
   * @return true when mainframe byte stream contains a record descriptor word (RDW).
   */
  public boolean hasRDW() {
    return rdw == null ? true : rdw;
  }

  /**
   * @return <code>byte[]</code> version of copybook.
   */
  public byte[] getCopybookBytes() {
    return copybook.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Provides a instance of copybook reader.
   * @return a instance <code>CopybookReader</code>.
   * @throws IOException thrown if there are any exception reading the copybook.
   */
  public CopybookReader getCopybookReader() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, getCodeFormat());
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(getCopybookBytes()));
    return new CopybookReader(streamCharSource, properties);
  }

  /**
   * Converts the cobol copybook into a Avro schema.
   * @param copybookReader containing parsed cobol copybook for converting to avro schema.
   * @return a instance of <code>Schema</code>.
   */
  public Schema getOutputSchema(CopybookReader copybookReader) {
    org.apache.avro.Schema avroSchema = copybookReader.getSchema();
    return AvroConverter.fromAvroSchema(avroSchema);
  }

  /**
   * Returns the schema validating all the requirements before returning.
   * @param failureCollector to record all the errors.
   * @return a instance of <code>Schema</code>.
   */
  public Schema getOutputSchemaAndValidate(FailureCollector failureCollector) {
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
      return getOutputSchema(copybookReader);
    } catch (Exception ex) {
      failureCollector.addFailure(String.format("Error while generating schema from the copybook: '%s'",
                                                ex.getMessage()), null)
        .withConfigProperty(PROPERTY_COPYBOOK)
        .withStacktrace(ex.getStackTrace());
      throw failureCollector.getOrThrowException();
    }
  }
}
