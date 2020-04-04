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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;

import javax.annotation.Nullable;

/**
 * Mainframe Reader plugin configuration.
 */
public class SourceConfig extends ConfigCommon {
  public static final String PROPERTY_REFERENCE = "referenceName";
  public static final String PROPERTY_FILEPATH = "filepath";
  public static final String PROPERTY_MAX_SPLIT_SIZE = "maxSplitSize";
  public static final long DEFAULT_MAX_SPLIT_SIZE_IN_MB = 1;
  public static final long CONVERT_TO_BYTES = 1024 * 1024;

  @Name(PROPERTY_REFERENCE)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;

  @Name(PROPERTY_FILEPATH)
  @Description("Complete path of the .bin to be read; for example: 'hdfs://10.222.41.31:9000/test/DTAR020_FB.bin' " +
    "or 'file:///home/cdap/DTAR020_FB.bin'.\n " +
    "This will be a fixed-length binary format file that matches the copybook.\n" +
    "(This is done to accept files present on a remote HDFS location.)")
  @Macro
  private String filepath;

  @Name(PROPERTY_MAX_SPLIT_SIZE)
  @Nullable
  @Description("Maximum split-size(MB) for each mapper in the MapReduce Job. Defaults to 1MB.")
  @Macro
  private Long maxSplitSize;

  public SourceConfig(String referenceName, String filepath, String copybook,
                      Long maxSplitSize, String charset, String codeFormat, Boolean rdw) {
    super(copybook, charset, codeFormat, rdw);
    this.referenceName = referenceName;
    this.filepath = filepath;
    this.maxSplitSize = maxSplitSize;
  }

  public String getFilepath() {
    return filepath;
  }

  public Long getMaxSplitSize() {
    return maxSplitSize != null ?
      maxSplitSize * CONVERT_TO_BYTES : DEFAULT_MAX_SPLIT_SIZE_IN_MB * CONVERT_TO_BYTES;
  }
}
