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

package io.cdap.plugin.mainframe;

import com.google.common.io.Resources;
import com.legstar.avro.cob2avro.io.AbstractZosDatumReader;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.mainframe.common.AvroConverter;
import io.cdap.plugin.mainframe.reader.CopybookReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 */
public class SimpleTest {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    URL copyBookURL = getClass().getClassLoader().getResource("custdat.cpbk");

    Assert.assertNotNull(copyBookURL);

    Properties properties =  new Properties();
    CopybookReader copybookReader = new CopybookReader(Resources.asCharSource(copyBookURL, StandardCharsets.UTF_8),
                                                       properties);

    URL dataURL = getClass().getClassLoader().getResource("custdat.bin");

    Assert.assertNotNull(dataURL);
    Schema schema = AvroConverter.fromAvroSchema(copybookReader.getSchema());

    try (AbstractZosDatumReader<GenericRecord> reader =
           copybookReader.createRecordReader(Resources.asByteSource(dataURL), "IBM01140", true)) {
      for (GenericRecord record : reader) {
        StructuredRecord structuredRecord = AvroConverter.fromAvroRecord(record, schema);
        LOG.trace(StructuredRecordStringConverter.toJsonString(structuredRecord));
      }
    }
  }

  @Test
  public void testCharset() throws Exception {
    SortedMap<String, Charset> charsets = Charset.availableCharsets();
    Set<String> strings = charsets.keySet();
    List<String> array = new ArrayList<>(strings);
    Collections.sort(array);
    for (String v : array) {
      System.out.println("\"" + v + "\",");
    }

  }
}
