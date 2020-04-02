/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.Numeric.Convert;
import net.sf.cb2xml.def.Cb2xmlConstants;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class to parse and read COBOL Copybook and binary data file contents.
 */
public class CopybookIOUtils {

  /**
   * Get the schema properties from the Copybook contents
   *
   * @param cblIs Input stream for COBOL Copybook contents
   * @return ExternalRecord object defining the schema fields and their properties
   * @throws RecordException if there is a problem in loading the copybook
   */
  public static ExternalRecord getExternalRecord(InputStream cblIs, String font) throws RecordException {
    CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_STANDARD_COLUMNS);
    CobolCopybookLoader copybookInt = new CobolCopybookLoader();
    return copybookInt.loadCopyBook(cblIs, "", CopybookLoader.SPLIT_NONE, 0, font, Convert.FMT_MAINFRAME, 0, null);
  }

  /**
   * Get record length for each line
   *
   * @param externalRecord ExternalRecord object defining the schema fields and their properties
   * @return the record length of each line
   */
  public static int getRecordLength(ExternalRecord externalRecord) {
    int recordByteLength = 0;
    Set<Integer> fieldPositions = new HashSet<>();
    for (ExternalField field : externalRecord.getRecordFields()) {
      if (!fieldPositions.contains(field.getPos())) {
        recordByteLength += field.getLen();
        fieldPositions.add(field.getPos());
     }
    }
    return recordByteLength;
  }
}
