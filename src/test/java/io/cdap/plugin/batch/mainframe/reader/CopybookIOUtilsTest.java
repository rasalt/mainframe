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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import net.sf.JRecord.Common.AbstractFieldValue;
import net.sf.JRecord.Common.BasicFileSchema;
import net.sf.JRecord.Common.CommonBits;
import net.sf.JRecord.Common.Constants;
import net.sf.JRecord.Common.IFieldDetail;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import net.sf.cb2xml.def.Cb2xmlConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class CopybookIOUtilsTest {

  private static String copyBook = "       01 WS-CTL-NETWORK-RECORD.\n" +
    "             02  WS-RECORD-TYPE              PIC X(05).\n" +
    "                     88 88-CTNW-DT    VALUE  'NTW00'.\n" +
    "                     88 88-PBNW-DT    VALUE  'NTW01'.\n" +
    "             02  WS-TRANSACTION-TYPE         PIC X(10).\n" +
    "             02  WS-CNTRL-NO-CTLNH       PIC 9(16).\n" +
    "             02  WS-CTLNH-DETAILS.\n" +
    "                  05 WS-AMPNTWK-ID-NO        PIC 9(9).\n" +
    "                  05 WS-CTLNH-EFF-DT         PIC X(10).\n" +
    "                  05 WS-CTLNH-POSTED-DTS     PIC X(26).\n" +
    "                  05 WS-CTLNH-EXP-DT         PIC X(10).\n" +
    "                  05 WS-CTLNH-ORGEFF-DT      PIC X(10).\n" +
    "                  05 WS-STATUS-CD-CTLNH      PIC X(2).\n" +
    "                     88 88-ACTIVE     VALUE '40'.\n" +
    "                     88 88-HISTORY    VALUE '10'.\n" +
    "                     88 88-CANCEL     VALUE '20'.\n" +
    "                     88 88-PREDATE    VALUE '50'.\n" +
    "                  05 WS-USER-ID              PIC X(08).\n" +
    "             02 FILLER                       PIC X(24).\n" +
    "       01  WS-PBNNTH-DETAILS REDEFINES WS-CTL-NETWORK-RECORD.\n" +
    "\n" +
    "             02  WS-RECORD-TYPE              PIC X(05).\n" +
    "             02  WS-TRANSACTION-TYPE         PIC X(10).\n" +
    "             02  WS-CNTRL-NO-PBNNTH      PIC 9(16).\n" +
    "             02  WS-PBNNTH-DATA.\n" +
    "                  05 WS-APMNTWK-ID-NO        PIC 9(9).\n" +
    "                  05 WS-BNFT-ID-CD           PIC X(5).\n" +
    "                  05 WS-PRBN-SEQ-NO          PIC 9(9).\n" +
    "                  05 WS-PBNNTH-EFF-DT        PIC X(10).\n" +
    "                  05 WS-PBNNTH-POSTED-DTS    PIC X(26).\n" +
    "                  05 WS-PBNNTH-EXP-DT        PIC X(10).\n" +
    "                  05 WS-PBNNTH-ORGEFF-DT     PIC X(10).\n" +
    "                  05 WS-STATUS-CD-PBNNTH     PIC X(2).\n" +
    "                     88 88-ACTIVE     VALUE '40'.\n" +
    "                     88 88-HISTORY    VALUE '10'.\n" +
    "                     88 88-CANCEL     VALUE '20'.\n" +
    "                     88 88-PREDATE    VALUE '50'.\n" +
    "                  05 WS-PBNNTH-USER-ID       PIC X(08).\n" +
    "             02  FILLER                      PIC X(10).";

  @Test
  public void testReadingFile() throws Exception {
    int fileStructure = Constants.IO_FIXED_LENGTH;

    InputStream inputStream = IOUtils.toInputStream(copyBook, "UTF-8");
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

    //CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_STANDARD_COLUMNS);
    CobolCopybookLoader loader = new CobolCopybookLoader();
    ExternalRecord externalRecord = loader.loadCopyBook(
      bufferedInputStream, "Myname", CopybookLoader.SPLIT_NONE, 0, "cp037", Convert.FMT_MAINFRAME, 0, null
    );
    LayoutDetail layout = externalRecord.asLayoutDetail();


    Map<String, IFieldDetail> fieldNameMap = layout.getFieldNameMap();

    int recordByteLength = 0;
    Set<Integer> fieldPositions = new HashSet<>();
    for (ExternalField field : externalRecord.getRecordFields()) {
      if (!fieldPositions.contains(field.getPos())) {
        recordByteLength += field.getLen();
        fieldPositions.add(field.getPos());
      }
    }

    Path path = new Path("/Users/nmotgi/Work/Demo/mainframe/R1Y9PB.FDR.FDRINCR.NETW.DAT.G0999V");
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    BufferedInputStream fileIn = new BufferedInputStream(fs.open(path));

    AbstractLineReader reader = LineIOProvider.getInstance()
      .getLineReader(BasicFileSchema.newFixedSchema(fileStructure));
    reader.open(fileIn, layout);

    java.nio.file.Path out = Paths.get("/tmp/cobol.test.txt");
    AbstractLine line;
    StringBuilder sb = new StringBuilder();
    while ((line = reader.read()) != null) {
      LinkedHashMap<String, AbstractFieldValue> value = new LinkedHashMap<>();
      for (ExternalField field : externalRecord.getRecordFields()) {
        AbstractFieldValue fieldValue = line.getFieldValue(field.getName());
        value.put(field.getName(), fieldValue);
      }
      Iterator<Map.Entry<String, AbstractFieldValue>> iterator = value.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, AbstractFieldValue> next = iterator.next();
        sb.append(next.getValue().asString()).append("|");
      }
      sb.append("\n");
    }
    Files.write(out, sb.toString().getBytes());
    reader.close();
    fileIn.close();
  };

  @Test
  public void testSchema() throws Exception {
    Schema schema = getOutputSchema(copyBook, "cp037");
    Assert.assertTrue(true);
  }

    private Schema getOutputSchema(String copybookContents, String font) {
    InputStream inputStream;
    ExternalRecord externalRecord;
    List<Schema.Field> fields = Lists.newArrayList();
    try {
      inputStream = IOUtils.toInputStream(copybookContents, "UTF-8");
      BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
      CommonBits.setDefaultCobolTextFormat(Cb2xmlConstants.USE_SUPPLIED_COLUMNS);
      CobolCopybookLoader copybookInt = new CobolCopybookLoader();
      externalRecord = copybookInt.loadCopyBook(bufferedInputStream, "", CopybookLoader.SPLIT_NONE, 0, font,
                                                       Convert.FMT_MAINFRAME, 0, null);
      String fieldName;
      for (ExternalField field : externalRecord.getRecordFields()) {
        fieldName = normalizeFieldName(field.getName());
        fields.add(Schema.Field.of(fieldName, Schema.nullableOf(
          Schema.of(Schema.Type.STRING))));
      }
      return Schema.recordOf("record", fields);
    } catch (IOException e) {
      throw new IllegalArgumentException("Exception while creating input stream for COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    } catch (RecordException e) {
      throw new IllegalArgumentException("Exception while creating record from COBOL Copybook. Invalid output " +
                                           "schema: " + e.getMessage(), e);
    }
  }

  private String normalizeFieldName(String fieldName) {
    return fieldName.replace("-", "_");
  }

  /**
   * Get the field values for the fields in the required format.
   * Date will be returned in the format - "yyyy-MM-dd".
   *
   * @param value AbstractFieldValue object to be converted in the JAVA primitive data types
   * @return data objects supported by CDAP
   */
  private Object getFieldValue(@Nullable AbstractFieldValue value) {
    if (value == null) {
      return null;
    }
    int type = value.getFieldDetail().getType();
    switch (type) {
      case 0:
        return value.asString();
      case 17:
        return value.asFloat();
      case 18:
      case 22:
      case 31:
      case 32:
      case 33:
        return value.asDouble();
      case 25:
        return value.asInt();
      case 35:
      case 36:
      case 39:
        return value.asLong();
      default:
        return value.asString();
    }
  }
}
