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
import java.io.IOException;

/**
 * This class <code>MainframeRecordReader</code>.
 */
public class MainframeRecordReader extends RecordReader<LongWritable, MainframeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MainframeRecordReader.class);
  private long start, end, position = 0;
  private MainframeReader reader;
  private LongWritable key = null;
  private MainframeRecord value = null;
  private ConfigProvider configProvider;

  public MainframeRecordReader(MainframeReader reader, ConfigProvider configProvider) {
    this.reader = reader;
    this.configProvider = configProvider;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Path path = new Path(configProvider.getBinaryFilePath());
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    FileSplit fileSplit = (FileSplit) split;
    BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit.getPath()));
    start = ((FileSplit) split).getStart();
    end = start + split.getLength();
    configProvider.setBinaryInputStream(fileIn);
    reader.initialize(configProvider);
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    key = new LongWritable(position);
    if (position > end) {
      return false;
    }
    value = reader.getRecord();
    position = position + value.getLength();
    return true;
  }

  @Override
  public MainframeRecord getCurrentValue() throws IOException, InterruptedException {
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
