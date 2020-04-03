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

import net.sf.JRecord.Common.AbstractFieldValue;
import net.sf.JRecord.Common.BasicFileSchema;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.Details.LayoutDetail;
import net.sf.JRecord.External.CobolCopybookLoader;
import net.sf.JRecord.External.CopybookLoader;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import net.sf.JRecord.IO.LineIOProvider;
import net.sf.JRecord.Numeric.Convert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Record Reader for CopybookReader plugin.
 * <p>
 * This will return the field name and value using the binary data and copybook contents, to be used as the
 * transform method input.
 */
public class CopybookRecordReader extends RecordReader<LongWritable, LinkedHashMap<String, AbstractFieldValue>> {
  private static final Logger LOG = LoggerFactory.getLogger(CopybookRecordReader.class);
  private AbstractLineReader reader;
  private ExternalRecord externalRecord;
  private int recordByteLength;
  private long start;
  private long position;
  private long end;
  private LongWritable key = null;
  private LinkedHashMap<String, AbstractFieldValue> value = null;
  private final String font;


  public CopybookRecordReader(String font) {
    this.font = font;
  }


  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // Get configuration
    Configuration conf = context.getConfiguration();
    int fileStructure = net.sf.JRecord.Common.Constants.IO_FIXED_LENGTH;
    Path path = new Path(conf.get(CopybookInputFormat.COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH));
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    // Create input stream for the COBOL copybook contents
    InputStream inputStream = IOUtils.toInputStream(
      conf.get(CopybookInputFormat.COPYBOOK_INPUTFORMAT_CBL_CONTENTS), "UTF-8"
    );
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

    try {
      CobolCopybookLoader loader = new CobolCopybookLoader();
      externalRecord = loader.loadCopyBook(
        bufferedInputStream, "", CopybookLoader.SPLIT_NONE, 0, font, Convert.FMT_MAINFRAME, 0, null
      );
      LayoutDetail layout = externalRecord.asLayoutDetail();

      recordByteLength = 0;
      Set<Integer> fieldPositions = new HashSet<>();
      for (ExternalField field : externalRecord.getRecordFields()) {
        if (!fieldPositions.contains(field.getPos())) {
          recordByteLength += field.getLen();
          fieldPositions.add(field.getPos());
        }
      }

      org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
        (org.apache.hadoop.mapreduce.lib.input.FileSplit) split;

      start = fileSplit.getStart();
      end = start + fileSplit.getLength();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit.getPath()));
      // Jump to the point in the split at which the first complete record of the split starts,
      // if not the first InputSplit
      if (start != 0) {
        position = start - (start % recordByteLength) + recordByteLength;
        fileIn.skip(position);
      }

      reader = LineIOProvider.getInstance()
        .getLineReader(BasicFileSchema.newFixedSchema(fileStructure, true, recordByteLength, "UTF-8"));
      reader.open(fileIn, layout);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private synchronized Pair<AbstractLine, ExternalField[]> readRecord() throws IOException {
    AbstractLine line = reader.read();
    return new Pair<>(line, externalRecord.getRecordFields());
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    LOG.info("Thread id : " + Thread.currentThread().getName());
    key = new LongWritable(position);
    Pair<AbstractLine, ExternalField[]> pair = readRecord();

    if (pair.getSecond().length < 1) {
      return false;
    }

    if (position > end) {
      return false;
    }

    position += recordByteLength;
    value = new LinkedHashMap<>();
    for (int i = 0; i < pair.getSecond().length; ++i) {
      String name = pair.getSecond()[i].getName();
      AbstractFieldValue fieldValue = pair.getFirst().getFieldValue(name);
      value.put(normalizeFieldName(name), fieldValue);
    }
    return true;
  }

  private String normalizeFieldName(String fieldName) {
    return fieldName.replace("-", "_");
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public LinkedHashMap<String, AbstractFieldValue> getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (position - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
