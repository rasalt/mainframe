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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;


/**
 * This class contains tests for <code>CopybookToSchema</code>.
 */
public class CopybookToSchemaTest {
  private static String copyBookWithRedefines = "       01 WS-CTL-NETWORK-RECORD.\n" +
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
  public void testBasicCopybookConversion() throws Exception {
    CopybookToSchema copybookToSchema = new CopybookToSchema(copyBookWithRedefines, "cp037");
    Schema schema = copybookToSchema.getSchema(true);
    Assert.assertTrue(true);
  }
}
