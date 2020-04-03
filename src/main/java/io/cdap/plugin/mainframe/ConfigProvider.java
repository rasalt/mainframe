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
package io.cdap.plugin.mainframe;

import io.cdap.plugin.batch.mainframe.reader.Pair;
import net.sf.JRecord.Details.IRecordDeciderX;
import net.sf.JRecord.Details.RecordDecider;
import net.sf.JRecord.JRecordInterface1;
import net.sf.JRecord.def.IO.builders.recordDeciders.IRecordDeciderBuilder;
import net.sf.JRecord.def.IO.builders.recordDeciders.ISingleFieldDeciderBuilder;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public final class ConfigProvider {
  private String layoutName;
  private String font;
  private String copyBookContent;
  private String binaryFilePath;
  private InputStream binaryInputStream;
  private IRecordDeciderX decider;

  private ConfigProvider(String layoutName, String font, String copyBookContent,
                         String binaryFilePath, IRecordDeciderX decider){
    this.layoutName = layoutName;
    this.font = font;
    this.copyBookContent = copyBookContent;
    this.binaryFilePath = binaryFilePath;
    this.decider = decider;
    this.binaryInputStream = null;
  }

  public String getLayoutName() {
    return layoutName;
  }

  public String getBinaryFilePath() {
    return binaryFilePath;
  }

  public void setBinaryInputStream(InputStream binaryInputStream) {
    this.binaryInputStream = binaryInputStream;
  }

  public InputStream getBinaryInputStream() throws IOException {
    if (binaryInputStream != null) {
      return binaryInputStream;
    }
    return new FileInputStream(binaryFilePath);
  }

  public String getCopyBookContent() {
    return copyBookContent;
  }

  public InputStream getCopybookInputStream() throws IOException  {
    return IOUtils.toInputStream(copyBookContent, "UTF-8");
  }

  public String getFont() {
    return font;
  }

  public RecordDecider getRecordDecider() {
    return decider;
  }

  public static class Builder {
    private String font;
    private String layoutName;
    private String binaryFilePath;
    private String copyBookContent;
    private InputStream copybookInputStream;
    private String deciderField;
    private String copybook = null;
    private List<Pair<String, String>> selectors = new ArrayList<>();

    public Builder(String layoutName, String font) {
      this.layoutName = layoutName;
      this.font = font;
    }

    public Builder setBinaryFilePath(String binaryFilePath) {
      this.binaryFilePath = binaryFilePath;
      return this;
    }

    public Builder setCopybookContent(String copyBookContent) {
      this.copyBookContent = copyBookContent;
      return this;
    }

    public Builder setDeciderField(String deciderField) {
      this.deciderField = deciderField;
      return this;
    }

    public Builder setRecordCondition(String deciderFieldValue, String recordName) {
      Pair<String, String> selector = new Pair<>(deciderFieldValue, recordName);
      selectors.add(selector);
      return this;
    }

    public ConfigProvider build() {
      IRecordDeciderBuilder decider = JRecordInterface1.RECORD_DECIDER_BUILDER;
      ISingleFieldDeciderBuilder builder = decider.singleFieldDeciderBuilder(deciderField, false);
      for(Pair<String, String> selector : selectors) {
        builder.addRecord(selector.getFirst(), selector.getSecond());
      }
      IRecordDeciderX build = selectors.size() > 0 ? builder.build() : null;
      return new ConfigProvider(layoutName, font, copyBookContent, binaryFilePath, build);
    }
  }
}

