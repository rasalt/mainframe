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

package io.cdap.plugin.mainframe.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.mainframe.reader.MainframeRecordReader;
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

/**
 * This class <code>MainFrameInputFormat</code> support FixedLength and Variable Length EBCDIC files.
 */
public class MainframeInputFormat extends FileInputFormat<LongWritable, StructuredRecord> {
  public static String cblCodeFormat = "mainframe.codeformat";
  public static String cblCharset = "mainframe.charset";
  public static String cblCopybook = "mainframe.copybook";
  public static String cblRdw = "mainframe.rdw";
  public static String cblFilePath = "mainframe.filepath";

  /**
   * Sets code format  for reading the mainframe file.
   *
   * @param job instance of <code>Job</code>.
   * @param charset encoding to be set for reader.
   */
  public static void setCharset(Job job, String charset) {
    job.getConfiguration().set(cblCharset, charset);
  }

  /**
   * Sets code format  for reading the mainframe file.
   *
   * @param job instance of <code>Job</code>.
   * @param codeFormat or encoding to be set for reader.
   */
  public static void setCodeFormat(Job job, String codeFormat) {
    job.getConfiguration().set(cblCodeFormat, codeFormat);
  }

  /**
   * Sets rdw.
   *
   * @param job instance of <code>Job</code>.
   * @param rdw or encoding to be set for reader.
   */
  public static void setRdw(Job job, Boolean rdw) {
    job.getConfiguration().setBoolean(cblRdw, rdw);
  }

  /**
   * Sets the text content of COBOL copybook.
   *
   * @param job instance of <code>Job</code>.
   * @param content of the COBOL copybook.
   */
  public static void setCopybook(Job job, String content) {
    job.getConfiguration().set(cblCopybook, content);
  }

  /**
   * Sets the input file path to be processed.
   *
   * @param job instance of <code>Job</code>.
   * @param binaryFilePath specifies the path to the input file or directory.
   */
  public static void setBinaryFilePath(Job job, String binaryFilePath) {
    job.getConfiguration().set(cblFilePath, binaryFilePath);
  }

  /**
   * Creates <code>MainFrameRecordReader</code> configuring it.
   *
   * @param split for which the record reader is created.
   * @param context task attempt context.
   * @return a instance of <code>RecordReader</code>
   */
  @Override
  public RecordReader<LongWritable, StructuredRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String copybook = conf.get(cblCopybook);
    String charset = conf.get(cblCharset);
    String filepath = conf.get(cblFilePath);
    String codeformat = conf.get(cblCodeFormat);
    boolean rdw = conf.getBoolean(cblRdw, false);
    return new MainframeRecordReader(copybook, charset, filepath, codeformat, rdw);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    Configuration conf = context.getConfiguration();
    Path path = new Path(conf.get(cblFilePath));
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
    return codec instanceof SplittableCompressionCodec;
  }
}
