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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Mainframe Reader plugin configuration.
 */
public class MainframeSourceConfig extends ReferencePluginConfig {

  public static final String REPLACEMENTS = "replacements";
  public static final String BINARY_FILE_PATH = "binaryFilePath";
  public static final String COPYBOOK_CONTENTS = "copybookContents";
  public static final String DROP = "drop";
  public static final String KEEP = "keep";
  public static final String MAX_SPLIT_SIZE = "maxSplitSize";
  public static final String CODEPAGE = "codepage";
  public static final String CHARSET = "charset";

  public static final long DEFAULT_MAX_SPLIT_SIZE_IN_MB = 1;
  public static final long CONVERT_TO_BYTES = 1024 * 1024;
  public static final Pattern ROW_TRIM_PATTERN = Pattern.compile("\\s+$");

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
  public static final String DEFAULT_FONT = "cp037";

  @Name(BINARY_FILE_PATH)
  @Description("Complete path of the .bin to be read; for example: 'hdfs://10.222.41.31:9000/test/DTAR020_FB.bin' " +
    "or 'file:///home/cdap/DTAR020_FB.bin'.\n " +
    "This will be a fixed-length binary format file that matches the copybook.\n" +
    "(This is done to accept files present on a remote HDFS location.)")
  @Macro
  private String binaryFilePath;

  @Name(COPYBOOK_CONTENTS)
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

  @Name(DROP)
  @Nullable
  @Description("Comma-separated list of fields to drop. For example: 'field1,field2,field3'. " +
    "If both fields to drop and fields to keep are given, fields to keep prevails.")
  private String drop;

  @Name(KEEP)
  @Nullable
  @Description("Comma-separated list of fields to keep. For example: 'field1,field2,field3'. " +
    "If both fields to drop and fields to keep are given, fields to keep prevails.")
  private String keep;

  @Name(MAX_SPLIT_SIZE)
  @Nullable
  @Description("Maximum split-size(MB) for each mapper in the MapReduce Job. Defaults to 1MB.")
  @Macro
  private Long maxSplitSize;

  @Name(CODEPAGE)
  @Nullable
  @Description("Code page to use - an alternative notation to the Charset. For example, cp037 or cp322. " +
    "If this is configured, it overrides the charset property.")
  @VisibleForTesting
  String codepage;

  @Name(CHARSET)
  @Nullable
  @Description("Charset used to read the data. Available Charset: EBCDIC-US (cp307), EBCDIC-Germany (cp237), " +
    "EBCDIC-Arabic (cp420), EBCDIC-Denmark, Norway (cp277), EBCDIC-France (cp297),\n" +
    "EBCDIC-Greece (cp875), EBCDIC-International (cp500), EBCDOC-Italy (cp280), EBCDIC-Russia (cp410)," +
    " EBCDIC-Spain (cp383), EBCDIC-Thailand (cp838), EBCDIC-Turkey (cp322).")
  @VisibleForTesting
  String charset;

  @Name(REPLACEMENTS)
  @Nullable
  @Description("List of placeholder strings and their replacements, for example, 'a=b,x=y'. All occurrences of " +
    "placeholders are replaced prior to loading the copybook. If after replacements a line of the copybook " +
    "exceeds 72 characters, the copybook will not be accepted and this will result in an error.")
  @VisibleForTesting
  String replacements;

  public MainframeSourceConfig(String referenceName, String binaryFilePath, String copybookContents, String drop,
                               String keep, Long maxSplitSize, String codepage, String charset, String replacements) {
    super(referenceName);
    this.binaryFilePath = binaryFilePath;
    this.copybookContents = copybookContents;
    this.drop = drop;
    this.keep = keep;
    this.maxSplitSize = maxSplitSize;
    this.codepage = codepage;
    this.charset = charset;
    this.replacements = replacements;
  }

  private MainframeSourceConfig(Builder builder) {
    super(builder.referenceName);
    binaryFilePath = builder.binaryFilePath;
    copybookContents = builder.copybookContents;
    drop = builder.drop;
    keep = builder.keep;
    maxSplitSize = builder.maxSplitSize;
    codepage = builder.codepage;
    charset = builder.charset;
    replacements = builder.replacements;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MainframeSourceConfig copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setBinaryFilePath(copy.binaryFilePath)
      .setCopybookContents(copy.copybookContents)
      .setDrop(copy.drop)
      .setKeep(copy.keep)
      .setMaxSplitSize(copy.maxSplitSize)
      .setCodepage(copy.codepage)
      .setCharset(copy.charset)
      .setReplacements(copy.replacements);
  }

  public String getBinaryFilePath() {
    return binaryFilePath;
  }

  public String getCopybookContents() {
    return copybookContents;
  }

  @Nullable
  public String getDrop() {
    return drop;
  }

  @Nullable
  public String getKeep() {
    return keep;
  }

  public Long getMaxSplitSize() {
    return maxSplitSize != null ? maxSplitSize * CONVERT_TO_BYTES : DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
  }

  @Nullable
  public String getCodepage() {
    return codepage;
  }

  @Nullable
  public String getCharset() {
    return charset;
  }

  @Nullable
  public String getReplacements() {
    return replacements;
  }

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


  @Nullable
  public Map<String, String> getReplacementsMap() {
    if (replacements == null) {
      return null;
    }
    Map<String, String> result = new HashMap<>();
    for (String pair : replacements.split(",")) {
      String[] subs = pair.split("=");
      result.put(subs[0].trim(), subs[1].trim());
    }
    return result.isEmpty() ? null : result;
  }

  public String getCopyBookContents() {
    String copybook = copybookContents;
    Map<String, String> replacementMap = getReplacementsMap();
    if (replacementMap != null) {
      for (Map.Entry<String, String> entry : replacementMap.entrySet()) {
        copybook = copybook.replaceAll(entry.getKey(), entry.getValue());
      }
    }
    return copybook;
  }

  public void validate(FailureCollector failureCollector) {
    if (!Strings.isNullOrEmpty(replacements)) {
      for (String pair : replacements.split(",")) {
        String[] subs = pair.split("=");
        if (subs.length != 2) {
          failureCollector.addFailure("Replacements must be of the form 'a=x,b=y,...'",
                                      "Provide Replacements in correct form.")
            .withConfigProperty(REPLACEMENTS);
          throw failureCollector.getOrThrowException();
        }
      }
    }
    if (!Strings.isNullOrEmpty(copybookContents)) {
      String copybook = getCopyBookContents();
      int lineNo = 0;
      for (String line : copybook.split("\\r?\\n")) {
        ++lineNo;
        for (int i = line.length() - 1; i > 0; i--) {
          String trimmed = ROW_TRIM_PATTERN.matcher(line).replaceAll("");
          if (trimmed.length() > 72) {
            String message = String.format("Copybook line '%d' exceeds the maximum length of 72 characters. " +
                                             "This may be caused by replacement strings that exceed the length " +
                                             "of the placeholders they substitute.", lineNo);
            failureCollector.addFailure(message, "Check your copybook and your replacements.")
              .withConfigProperty(COPYBOOK_CONTENTS)
              .withConfigProperty(REPLACEMENTS);
            break;
          }
        }
      }
    }
    validateSchema(failureCollector);
  }

  private void validateSchema(FailureCollector failureCollector) {
    List<Schema.Field> fields = Lists.newArrayList();
    ExternalRecord externalRecord;
    try (InputStream inputStream = IOUtils.toInputStream(getCopyBookContents(), "UTF-8")) {
      BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      externalRecord = CopybookIOUtils.getExternalRecord(bufferedInputStream, getFont());
      String fieldName;
      for (ExternalField field : externalRecord.getRecordFields()) {
        fieldName = field.getName().replace("-", "_");
        if (keep != null && !keep.contains(fieldName)) {
          continue;
        }
        if (drop != null && drop.contains(fieldName)) {
          continue;
        }
        fields.add(Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(getFieldSchemaType(field.getType())))));
      }
      Schema.recordOf("record", fields);
    } catch (IOException e) {
      failureCollector.addFailure("Exception while creating input stream for COBOL Copybook.",
                                  "Check COBOL Copybook data.")
        .withConfigProperty(COPYBOOK_CONTENTS)
        .withStacktrace(e.getStackTrace());
    } catch (RecordException e) {
      failureCollector.addFailure("Exception while creating record from COBOL Copybook.",
                                  "Check COBOL Copybook data.")
        .withConfigProperty(COPYBOOK_CONTENTS)
        .withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Get the field Schema.Type from the copybook data types
   *
   * @param type AbstractFiledValue type to be converted to CDAP Schema.Type
   * @return CDAP Schema.Type objects
   */
  public static Schema.Type getFieldSchemaType(int type) {
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
   * Builder for creating a {@link MainframeSourceConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String copybookContents;
    private String codepage;
    private String charset;
    private String replacements;
    private String binaryFilePath;
    private String drop;
    private String keep;
    private Long maxSplitSize;

    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setBinaryFilePath(String binaryFilePath) {
      this.binaryFilePath = binaryFilePath;
      return this;
    }

    public Builder setCopybookContents(String copybookContents) {
      this.copybookContents = copybookContents;
      return this;
    }

    public Builder setDrop(String drop) {
      this.drop = drop;
      return this;
    }

    public Builder setKeep(String keep) {
      this.keep = keep;
      return this;
    }

    public Builder setMaxSplitSize(Long maxSplitSize) {
      this.maxSplitSize = maxSplitSize;
      return this;
    }

    public Builder setCodepage(String codepage) {
      this.codepage = codepage;
      return this;
    }

    public Builder setCharset(String charset) {
      this.charset = charset;
      return this;
    }

    public Builder setReplacements(String replacements) {
      this.replacements = replacements;
      return this;
    }

    public MainframeSourceConfig build() {
      return new MainframeSourceConfig(this);
    }
  }
}
