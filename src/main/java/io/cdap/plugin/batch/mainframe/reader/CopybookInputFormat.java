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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * InputFormat class for CopybookReader plugin.
 */
public class CopybookInputFormat extends FileInputFormat<LongWritable, LinkedHashMap<String, AbstractFieldValue>> {
  public static final String COPYBOOK_INPUTFORMAT_CBL_CONTENTS = "copybook.inputformat.cbl.contents";
  public static final String COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH = "copybook.inputformat.data.hdfs.path";
  public static final String COPYBOOK_INPUTFORMAT_DATA_FONT = "copybook.inputformat.data.font";

  public static void setCopybookInputformatCharset(Job job, String font) {
    // font for copybook data cp037 for EBCDIC US.
    job.getConfiguration().set(COPYBOOK_INPUTFORMAT_DATA_FONT, font);
  }

  public static void setCopybookInputformatCblContents(Job job, String copybookCOntents) {
    job.getConfiguration().set(COPYBOOK_INPUTFORMAT_CBL_CONTENTS, copybookCOntents);
  }

  public static void setBinaryFilePath(Job job, String binaryFile) {
    job.getConfiguration().set(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH, binaryFile);
  }

  @Override
  public RecordReader<LongWritable, LinkedHashMap<String, AbstractFieldValue>>
  createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    String font = context.getConfiguration().get(COPYBOOK_INPUTFORMAT_DATA_FONT);
    return new CopybookRecordReader(font);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    Configuration conf = context.getConfiguration();
    Path path = new Path(conf.get(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH));
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
    return (null == codec) || codec instanceof SplittableCompressionCodec;
  }
}
