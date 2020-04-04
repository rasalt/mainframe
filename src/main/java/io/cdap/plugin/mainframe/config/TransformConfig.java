/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
 * <code>CobolRecordConvertorConfig</code> provides configuration for <code>CobolRecordConverter</code>.
 */
public class TransformConfig extends ConfigCommon {
  public static final String PROPERTY_CONTENT_FIELD_NAME = "contentFieldName";

  @Name(PROPERTY_CONTENT_FIELD_NAME)
  @Description("Name of the field containing COBOL records")
  @Macro
  private final String contentFieldName;

  public TransformConfig(String copybook, @Nullable String charset, @Nullable String codeFormat,
                         @Nullable Boolean rdw, String contentFieldName) {
    super(copybook, charset, codeFormat, rdw);
    this.contentFieldName = contentFieldName;
  }

  public String getContentFieldName() {
    return contentFieldName;
  }
}
