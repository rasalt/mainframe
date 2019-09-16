/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants;
import net.sf.JRecord.Common.RecordException;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.Details.Line;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.External.ToLayoutDetail;
import net.sf.JRecord.IO.AbstractLineWriter;
import net.sf.JRecord.IO.LineIOProvider;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Unit test for {@link MainframeSource} classes.
 */

public class MainframeReaderTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("6.0.0-SNAPSHOT");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("etlbatch", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary ETLBATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String COPYBOOK_CONTENTS = "" +
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
    "001200        03  DTAR020-DATE               PIC S9(07)   COMP-3.               \n" +
    "001300        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               \n" +
    "001400        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               \n" +
    "001500        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.";

  // a copybook that uses REDEFINES to also make the store available as a single string.
  // the one above exposes keycode-no (8 chars = bytes) and store-no (3 digits comp-3 = 2 bytes)
  // this one additionally exposes a 10-char kcode-string. This does not make much sense
  // in real life (interpreting binary nibbles as characters) but works for testing here.
  // Note that for this to work, the store-no must consist of numbers that are valid EBCDIC-US chars.
  private static final String COPYBOOK_WITH_REDEFINE = "" +
    "000100*                                                                         \n" +
    "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
    "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
    "000400*                                                                         \n" +
    "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
    "000600*                                                                         \n" +
    "000700*   RECORD LENGTH IS 27.                                                  \n" +
    "000800*                                                                         \n" +
    "000900        03  DTAR020-KCODE-STORE-ID.                                       \n" +
    "000950            05 DTAR020-KCODE-STRING    PIC X(10).                         \n" +
    "001000        03  DTAR020-KCODE-STORE-KEY REDEFINES DTAR020-KCODE-STORE-ID.     \n" +
    "001100            05 DTAR020-KEYCODE-NO      PIC X(08).                         \n" +
    "001200            05 DTAR020-STORE-NO        PIC S9(03)   COMP-3.               \n" +
    "001300        03  DTAR020-DATE               PIC S9(07)   COMP-3.               \n" +
    "001400        03  DTAR020-DEPT-NO            PIC S9(03)   COMP-3.               \n" +
    "001500        03  DTAR020-QTY-SOLD           PIC S9(9)    COMP-3.               \n" +
    "001600        03  DTAR020-SALE-PRICE         PIC S9(9)V99 COMP-3.";

  // After replacing :PREFIX: and "COMP: with DTAR020 and COMP-3,
  // this copybook should yield the same outputs as COPYBOOK_CONTENTS
  private static final String COPYBOOK_WITH_REPLACEMENTS = "" +
    "000100*                                                                         \n" +
    "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
    "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
    "000400*                                                                         \n" +
    "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
    "000600*                                                                         \n" +
    "000700*   RECORD LENGTH IS 27.                                                  \n" +
    "000800*                                                                         \n" +
    "000900        03  :PREFIX:-KCODE-STORE-KEY.                                      \n" +
    "001000            05 :PREFIX:-KEYCODE-NO      PIC X(08).                         \n" +
    "001100            05 :PREFIX:-STORE-NO        PIC S9(03)   :COMP:.               \n" +
    "001200        03  :PREFIX:-DATE               PIC S9(07)   :COMP:.               \n" +
    "001300        03  :PREFIX:-DEPT-NO            PIC S9(03)   :COMP:.               \n" +
    "001400        03  :PREFIX:-QTY-SOLD           PIC S9(9)    :COMP:.               \n" +
    "001500        03  :PREFIX:-SALE-PRICE         PIC S9(9)V99 :COMP:.";

  private static final Set<String> ALL_FIELDS = ImmutableSet.of("DTAR020_KEYCODE_NO",
                                                                "DTAR020_STORE_NO",
                                                                "DTAR020_DATE",
                                                                "DTAR020_DEPT_NO",
                                                                "DTAR020_QTY_SOLD",
                                                                "DTAR020_SALE_PRICE");

  private static final List<Map<String, Object>> FILE_CONTENTS = ImmutableList.<Map<String, Object>>of(
    ImmutableMap.<String, Object>builder()
      .put("DTAR020_KCODE_STRING", "69694158a*")
      .put("DTAR020_KEYCODE_NO", "69694158")
      .put("DTAR020_STORE_NO", 815.0)
      .put("DTAR020_DATE", 40118.0)
      .put("DTAR020_DEPT_NO", 280.0)
      .put("DTAR020_QTY_SOLD", 1.0)
      .put("DTAR020_SALE_PRICE", 5.01).build(),
    ImmutableMap.<String, Object>builder()
      .put("DTAR020_KCODE_STRING", "69694158b*")
      .put("DTAR020_KEYCODE_NO", "63604808")
      .put("DTAR020_STORE_NO", 825.0)
      .put("DTAR020_DATE", 40118.0)
      .put("DTAR020_DEPT_NO", 170.0)
      .put("DTAR020_QTY_SOLD", 1.0)
      .put("DTAR020_SALE_PRICE", 4.87).build());

  @BeforeClass
  public static void setupTest() throws Exception {
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);
    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("copybookreader-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      MainframeSource.class);
    FileInputFormat.setInputPaths(new JobConf(), new Path("src/test/resources"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    temporaryFolder.delete();
  }

  @Test
  public void testCopybookReaderWithDropOrKeep() throws Exception {
    testCopybookReaderWithFields("test_1", COPYBOOK_CONTENTS, null, null, ALL_FIELDS);
    testCopybookReaderWithFields("test_2", COPYBOOK_CONTENTS, null, "DTAR020_KEYCODE_NO,DTAR020_DATE,DTAR020_STORE_NO",
                                 ImmutableSet.of("DTAR020_DEPT_NO", "DTAR020_QTY_SOLD", "DTAR020_SALE_PRICE"));
    testCopybookReaderWithFields("test_3", COPYBOOK_CONTENTS, "DTAR020_KEYCODE_NO,DTAR020_DATE,DTAR020_STORE_NO", null,
                                 ImmutableSet.of("DTAR020_KEYCODE_NO", "DTAR020_DATE", "DTAR020_STORE_NO"));
    testCopybookReaderWithFields("test_4", COPYBOOK_CONTENTS, "DTAR020_KEYCODE_NO,DTAR020_DATE", "DTAR020_DATE",
                                 ImmutableSet.of("DTAR020_KEYCODE_NO", "DTAR020_DATE"));
  }

  @Test
  public void testCopybookReaderWithReplacements() throws Exception {
    testCopybookReaderWithFields("test_5", COPYBOOK_CONTENTS,
                                 ":PREFIX:=DTAR020, :COMP:=COMP-3",
                                 "DTAR020_KEYCODE_NO,DTAR020_DATE", "DTAR020_DATE",
                                 ImmutableSet.of("DTAR020_KEYCODE_NO", "DTAR020_DATE"));
  }

  @Test(expected = IllegalStateException.class)
  public void testCopybookReaderWithRedefines() throws Exception {
    testCopybookReaderWithFields("test_6", COPYBOOK_WITH_REDEFINE,
                                 "DTAR020_KEYCODE_NO,DTAR020_STORE_NO,DTAR020_KCODE_STRING", null,
                                 ImmutableSet.of("DTAR020_KEYCODE_NO", "DTAR020_STORE_NO", "DTAR020_KCODE_STRING"));
  }

  public void testCopybookReaderWithFields(String testName, String copybook,
                                           @Nullable String keep, @Nullable String drop,
                                           Set<String> expectedFields) throws Exception {
    testCopybookReaderWithFields(testName, copybook, null, keep, drop, expectedFields);
  }

  public void testCopybookReaderWithFields(String testName, String copybook, String replacements,
                                           @Nullable String keep, @Nullable String drop,
                                           Set<String> expectedFields) throws Exception {

    ImmutableMap.Builder<String, String> propsBulder = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      // .put("binaryFilePath", "src/test/resources/DTAR020_FB.bin")
      .put("binaryFilePath", "src/test/resources/DTAR020_F8-modified.bin")
      .put("copybookContents", copybook);
    if (keep != null) {
      propsBulder.put("keep", keep);
    }
    if (drop != null) {
      propsBulder.put("drop", drop);
    }
    if (replacements != null) {
      propsBulder.put("replacements", replacements);
    }
    Map<String, String> sourceProperties = propsBulder.build();

    ETLStage source = new ETLStage("MainframeReader", new ETLPlugin("MainframeReader", BatchSource.PLUGIN_TYPE,
                                                                    sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MainframeReaderTest_" + testName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());
    validateResult(0, expectedFields, FILE_CONTENTS.get(0), output.get(0));
    validateResult(1, expectedFields, FILE_CONTENTS.get(1), output.get(1));

    MockSink.clear(outputManager);
  }

  private void validateResult(int i, Set<String> expectedFields,
                              Map<String, Object> expected, StructuredRecord result) {
    for (String field : ALL_FIELDS) {
      if (expectedFields.contains(field)) {
        Assert.assertEquals("Mismatch for field '" + field + "' in result #" + i,
                            expected.get(field), result.get(field));
      } else {
        Assert.assertNull("Unxpected field '" + field + "' in result #" + i, result.get(field));
      }
    }
  }

  @Test
  public void testMainframeReaderWithAllFields() throws Exception {

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("DTAR020_KEYCODE_NO", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("DTAR020_STORE_NO", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_DATE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_DEPT_NO", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_QTY_SOLD", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_SALE_PRICE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", "src/test/resources/DTAR020_FB.bin")
      .put("copybookContents", COPYBOOK_CONTENTS)
      .put("maxSplitSize", "5")
      .build();

    ETLStage source = new ETLStage("MainframeReader", new ETLPlugin("MainframeReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsource-test-without-schema";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MainframeReaderTest_test_7");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    Assert.assertEquals("Expected records", 2, output.size());

    Map<String, Double> result = new HashMap<>();
    result.put((String) output.get(0).get("DTAR020_KEYCODE_NO"), (Double) output.get(0).get("DTAR020_SALE_PRICE"));
    result.put((String) output.get(1).get("DTAR020_KEYCODE_NO"), (Double) output.get(1).get("DTAR020_SALE_PRICE"));

    Assert.assertEquals(4.87, result.get("63604808").doubleValue(), 0.1);
    Assert.assertEquals(5.01, result.get("69694158").doubleValue(), 0.1);
    Assert.assertEquals("Expected schema", output.get(0).getSchema(), schema);
  }

  @Test
  public void testInvalidMainframeReaderSource() throws Exception {

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", "src/test/resources/DTAR020_FB.txt")
      .build();

    ETLStage source = new ETLStage("MainframeReader", new ETLPlugin("MainframeReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsource-test-incorrect-schema";
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MainframeReaderTest_test_8");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (IllegalStateException e) {
      // expected - since cobol copybook is required
    }
  }

  @Test
  public void testDefaults() {
    MainframeSource.MainframeSourceConfig mainframeSourceConfig = new MainframeSource.MainframeSourceConfig();
    Assert.assertEquals(Long.toString(MainframeSource.DEFAULT_MAX_SPLIT_SIZE_IN_MB * 1024 * 1024),
                        mainframeSourceConfig.getMaxSplitSize().toString());
  }

  @Test
  public void testDataTypes() throws Exception {

    String copybookContents = "000100*                                                                         \n" +
      "000200*   DTAR020 IS THE OUTPUT FROM DTAB020 FROM THE IML                       \n" +
      "000300*   CENTRAL REPORTING SYSTEM                                              \n" +
      "000400*                                                                         \n" +
      "000500*   CREATED BY BRUCE ARTHUR  19/12/90                                     \n" +
      "000600*                                                                         \n" +
      "000700*   RECORD LENGTH IS 27.                                                  \n" +
      "000800*                                                                         \n" +
      "000900        03  DTAR020-KCODE-STORE-KEY.                                      \n" +
      "001000            05 DTAR020-KEYCODE      \tPIC X(01).    \n" +
      "001100            05 DTAR020-STORE-NO        \tPIC 9(18) \t\tCOMP.               \n" +
      "001200        03  DTAR020-DATE               \tPIC S9(08)  \tCOMP-3.               \n" +
      "001300        03  DTAR020-DEPT-NO            \tpic S9(5)   \tBINARY.               \n" +
      "001400        03  DTAR020-QTY-SOLD           \tPIC S9(9)   \tCOMP-3.               \n" +
      "001500        03  DTAR020-SALE-PRICE         \tPIC S9(9)V99 \tCOMP-1.    \n" +
      "001600        03  DTAR020-MRP         \t\t \tPIC S9(9)V99\tCOMP-2.      \n" +
      "001700        03  DTAR020-ZONED-DECIMAL      \tPIC S9(5)V9.   \n" +
      "001800        03  DTAR020-ASSUMED-DECIMAL    \tPIC 999V99.   \n" +
      "001900        03  DTAR020-NUM-RIGHTJUSTIFIED \tPIC 9(5).";

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("DTAR020_KEYCODE", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("DTAR020_STORE_NO", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("DTAR020_DATE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_DEPT_NO", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("DTAR020_QTY_SOLD", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_SALE_PRICE", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("DTAR020_MRP", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_ZONED_DECIMAL", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_ASSUMED_DECIMAL", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("DTAR020_NUM_RIGHTJUSTIFIED", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, "TestCase")
      .put("binaryFilePath", generateBinaryFile(copybookContents))
      .put("copybookContents", copybookContents)
      .build();

    ETLStage source = new ETLStage("MainframeReader", new ETLPlugin("MainframeReader", BatchSource.PLUGIN_TYPE,
                                                                   sourceProperties, null));

    String outputDatasetName = "output-batchsource-test-datatypes";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("MainframeReaderTest_test_9");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    StructuredRecord record = output.get(0);

    Assert.assertEquals("1", record.get("DTAR020_KEYCODE"));
    Assert.assertEquals(123, (int) record.get("DTAR020_NUM_RIGHTJUSTIFIED"));
    Assert.assertEquals(20L, (long) record.get("DTAR020_STORE_NO"));
    Assert.assertEquals(20140202, (Double) record.get("DTAR020_DATE"), 0);
    Assert.assertEquals(100L, (long) record.get("DTAR020_DEPT_NO"));
    Assert.assertEquals(7, (Double) record.get("DTAR020_QTY_SOLD"), 0);
    Assert.assertEquals(7.15F, (Float) record.get("DTAR020_SALE_PRICE"), 0);
    Assert.assertEquals(7.30, (Double) record.get("DTAR020_MRP"), 0);
    Assert.assertEquals(-123.2, (Double) record.get("DTAR020_ZONED_DECIMAL"), 0);
    Assert.assertEquals(134.25, (Double) record.get("DTAR020_ASSUMED_DECIMAL"), 0);

    Assert.assertEquals("Expected schema", record.getSchema(), schema);
  }


  private String generateBinaryFile(String copybook) {
    ExternalRecord externalRecord = null;
    try {
      File file = temporaryFolder.newFile("TestFile.bin");
      String binaryFilePath = file.getPath();
      int fileStructure = net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH;
      InputStream inputStream = IOUtils.toInputStream(copybook, "UTF-8");
      // Testing with charset cp037 for EBCDIC US
      externalRecord = CopybookIOUtils.getExternalRecord(inputStream, "cp037");
      LayoutDetail layout = ToLayoutDetail.getInstance().getLayout(externalRecord);
      AbstractLineWriter writer = LineIOProvider.getInstance().getLineWriter(fileStructure);
      AbstractLine saleRecord = new Line(layout);
      writer.open(binaryFilePath);
      saleRecord.getFieldValue("DTAR020-KEYCODE").set(1);
      saleRecord.getFieldValue("DTAR020-NUM-RIGHTJUSTIFIED").set(123);
      saleRecord.getFieldValue("DTAR020-STORE-NO").set(20);
      saleRecord.getFieldValue("DTAR020-DATE").set(20140202);
      saleRecord.getFieldValue("DTAR020-DEPT-NO").set(100);
      saleRecord.getFieldValue("DTAR020-QTY-SOLD").set(7);
      saleRecord.getFieldValue("DTAR020-SALE-PRICE").set(7.15);
      saleRecord.getFieldValue("DTAR020-MRP").set(7.30);
      saleRecord.getFieldValue("DTAR020-ZONED-DECIMAL").set(-123.2);
      saleRecord.getFieldValue("DTAR020-ASSUMED-DECIMAL").set(134.25);

      writer.write(saleRecord);
      writer.close();
      return binaryFilePath;
    } catch (RecordException e) {
      throw new IllegalArgumentException("Invalid copybook contents: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error creating binary test file: " + e.getMessage(), e);
    }
  }

  @Test
  public void testConfig() {
    MainframeSource.MainframeSourceConfig config = new MainframeSource.MainframeSourceConfig();
    Assert.assertEquals(MainframeSource.MainframeSourceConfig.DEFAULT_FONT, config.getFont());
    config.charset = "EBCDIC-International";
    Assert.assertEquals("cp500", config.getFont());
    config.codepage = "cp297";
    Assert.assertEquals("cp297", config.getFont());
    config.charset = null;
    Assert.assertEquals("cp297", config.getFont());

    config.copybookContents = COPYBOOK_CONTENTS;
    Assert.assertEquals(COPYBOOK_CONTENTS, config.getCopyBookContents());

    config.copybookContents = COPYBOOK_WITH_REPLACEMENTS;
    config.replacements = ":PREFIX:=DTAR020, :COMP:=COMP-3";
    Assert.assertEquals(ImmutableMap.of(":PREFIX:", "DTAR020", ":COMP:", "COMP-3"), config.getReplacements());
    Assert.assertEquals(COPYBOOK_CONTENTS, config.getCopyBookContents());

    config.replacements = "a=b,c";
    try {
      config.getReplacements();
      Assert.fail("expected illegal argument");
    } catch (IllegalArgumentException e) {
      // expected
    }

    config.replacements = ":PREFIX:=DTAR020ANDSOMEMORELENGHTYTEXT, :COMP:=COMP-3";
    try {
      config.getCopyBookContents();
      Assert.fail("expected illegal argument");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}


