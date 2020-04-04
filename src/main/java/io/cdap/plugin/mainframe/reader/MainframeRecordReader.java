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

package io.cdap.plugin.mainframe.reader;

import com.legstar.avro.cob2avro.io.AbstractZosDatumReader;
import com.legstar.cob2xsd.Cob2XsdConfig;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.mainframe.common.AvroConverter;
import io.cdap.plugin.mainframe.common.StreamByteSource;
import io.cdap.plugin.mainframe.common.StreamCharSource;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * This class <code>MainframeRecordReader</code>.
 */
public class MainframeRecordReader extends RecordReader<LongWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MainframeRecordReader.class);
  private long position = 0;
  private LongWritable key = null;
  private StructuredRecord value;
  private String copybook;
  private String charset;
  private String filepath;
  private String codeFormat;
  private Boolean rdw;
  AbstractZosDatumReader<GenericRecord> reader;
  private Schema schema;

  public MainframeRecordReader(String copybook, String charset, String filepath, String codeFormat, Boolean rdw) {
    this.copybook = copybook;
    this.charset = charset;
    this.filepath = filepath;
    this.codeFormat = codeFormat;
    this.rdw = rdw;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Path path = new Path(filepath);
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    Properties properties = new Properties();
    properties.setProperty(Cob2XsdConfig.CODE_FORMAT, codeFormat);
    StreamCharSource streamCharSource
      = new StreamCharSource(new ByteArrayInputStream(copybook.getBytes(StandardCharsets.UTF_8)));
    CopybookReader copybookReader = new CopybookReader(streamCharSource, properties);
    schema = getOutputSchemaAndValidate(copybookReader);

    FileSplit fileSplit = (FileSplit) split;
    BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit.getPath()));

    StreamByteSource source = new StreamByteSource(fileIn, split.getLength());
    reader = copybookReader.createRecordReader(source, charset, rdw);
  }

  public Schema getOutputSchemaAndValidate(CopybookReader copybookReader) {
    org.apache.avro.Schema avroSchema = copybookReader.getSchema();
    return AvroConverter.fromAvroSchema(avroSchema);
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    key = new LongWritable(position);

    if (!reader.hasNext()) {
      return false;
    }
    value = AvroConverter.fromAvroRecord(reader.next(), schema);
    position++;
    return true;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0f;
  }

  @Override
  public void close() throws IOException {

  }
}
