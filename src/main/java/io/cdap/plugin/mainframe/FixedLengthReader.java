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

import com.google.common.io.Closeables;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.FieldDetail;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.RecordDetail;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.JRecordInterface1;
import net.sf.JRecord.def.IO.builders.ICobolIOBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * This class <code>FixedLengthParser</code> parser.
 */
public class FixedLengthReader implements MainframeReader {
  private AbstractLineReader reader;
  private AbstractLine line;
  private InputStream inputStream;
  private LayoutDetail layout;

  public FixedLengthReader() {
    this.inputStream = null;
  }

  public void initialize(ConfigProvider provider) throws IOException {
    ICobolIOBuilder iCobolIOBuilder = JRecordInterface1.COBOL
      .newIOBuilder(provider.getCopybookInputStream(), provider.getLayoutName())
      .setFileOrganization(Constants.IO_FIXED_LENGTH)
      .setFont(provider.getFont())
      .setSplitCopybook(CopybookLoader.SPLIT_01_LEVEL);

    if (provider.getRecordDecider() != null) {
      iCobolIOBuilder.setRecordDecider(provider.getRecordDecider());
    }

    inputStream = provider.getBinaryInputStream();
    reader = iCobolIOBuilder.newReader(inputStream);
    layout = iCobolIOBuilder.getLayout();
  }

  @Override
  public MainframeRecord getRecord() throws IOException {
    MainframeRecord.Builder builder = new MainframeRecord.Builder();
    line = reader.read();
    if (line == null) {
      return builder.build();
    }
    RecordDetail details = layout.getRecord(line.getPreferredLayoutIdxAlt());
    builder.setLength(details.getLength());
    builder.setRecordName(details.getRecordName());
    List<FieldDetail> fields = details.getFields();
    for (FieldDetail field : fields) {
      builder.add(new Datum(field.getName(), line.getFieldValue(field.getName())));
    }
    return builder.build();
  }

  public void close() {
    Closeables.closeQuietly(inputStream);
  }
}
