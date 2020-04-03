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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mainframe Reader plugin configuration.
 */
public class Config extends ReferencePluginConfig {

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
  @Macro
  String copybookContents;

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
  @Macro
  String codepage;

  @Name(CHARSET)
  @Nullable
  @Description("Charset used to read the data. Available Charset: EBCDIC-US (cp307), EBCDIC-Germany (cp237), " +
    "EBCDIC-Arabic (cp420), EBCDIC-Denmark, Norway (cp277), EBCDIC-France (cp297),\n" +
    "EBCDIC-Greece (cp875), EBCDIC-International (cp500), EBCDOC-Italy (cp280), EBCDIC-Russia (cp410)," +
    " EBCDIC-Spain (cp383), EBCDIC-Thailand (cp838), EBCDIC-Turkey (cp322).")
  @VisibleForTesting
  @Macro
  String charset;

  @Name("decider-field-name")
  @Nullable
  @Description("Name of the field used to decide the split of records. E.g. WS-RECORD-TYPE.")
  @Macro
  String deciderField;

  @Name("selectors")
  @Nullable
  @Description("Specify the value of decider field and the record it's associated with.")
  @Macro
  String selectors;

  public Config(String referenceName, String binaryFilePath, String copybookContents, String drop,
                String keep, Long maxSplitSize, String codepage, String charset,
                String deciderField, String selectors) {
    super(referenceName);
    this.binaryFilePath = binaryFilePath;
    this.copybookContents = copybookContents;
    this.maxSplitSize = maxSplitSize;
    this.codepage = codepage;
    this.charset = charset;
    this.deciderField = deciderField;
    this.selectors = selectors;
  }

  private Config(Builder builder) {
    super(builder.referenceName);
    binaryFilePath = builder.binaryFilePath;
    copybookContents = builder.copybookContents;
    maxSplitSize = builder.maxSplitSize;
    codepage = builder.codepage;
    charset = builder.charset;
    deciderField = builder.deciderField;
    deciderField = builder.selectors;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(Config copy) {
    return builder()
      .setReferenceName(copy.referenceName)
      .setBinaryFilePath(copy.binaryFilePath)
      .setCopybookContents(copy.copybookContents)
      .setMaxSplitSize(copy.maxSplitSize)
      .setCodepage(copy.codepage)
      .setCharset(copy.charset);
  }

  public String getBinaryFilePath() {
    return binaryFilePath;
  }

  public String getCopybookContents() {
    return copybookContents;
  }

  public Long getMaxSplitSize() {
    return maxSplitSize != null ? maxSplitSize * CONVERT_TO_BYTES : DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
  }

  public String getDeciderField() {
    return deciderField;
  }

  public String getSelectors() {
    return selectors;
  }

  @Nullable
  public String getCodepage() {
    return codepage;
  }

  @Nullable
  public String getCharset() {
    return charset;
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

  public String getCopyBookContents() {
    return copybookContents;
  }

  /**
   * Builder for creating a {@link Config}.
   */
  public static final class Builder {
    private String referenceName;
    private String copybookContents;
    private String codepage;
    private String charset;
    private String binaryFilePath;
    private Long maxSplitSize;
    private String deciderField;
    private String selectors;

    private Builder() {
    }

    public Builder setSelectors(String selectors) {
      this.selectors = selectors;
      return this;
    }

    public Builder setDeciderField(String deciderField) {
      this.deciderField = deciderField;
      return this;
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

    public Config build() {
      return new Config(this);
    }
  }
}
