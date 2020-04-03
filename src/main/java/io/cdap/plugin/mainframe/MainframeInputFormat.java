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

package io.cdap.plugin.mainframe;

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
public class MainframeInputFormat extends FileInputFormat<LongWritable, MainframeRecord> {
  public static String cblFont = "mainframe.font";
  public static String cblContent = "mainframe.copybook";
  public static String cblDeciderKey = "mainframe.decider.key";
  public static String cblSelector = "mainframe.selectors";
  public static String cblFileStructure = "mainframe.structure";
  public static String cblBinaryFilePath = "mainframe.binary.path";

  /**
   * Sets font (encoding) for reading the mainframe file.
   *
   * @param job instance of <code>Job</code>.
   * @param font or encoding to be set for reader.
   */
  public static void setFont(Job job, String font) {
    job.getConfiguration().set(cblFont, font);
  }

  /**
   * Sets the text content of COBOL copybook.
   *
   * @param job instance of <code>Job</code>.
   * @param content of the COBOL copybook.
   */
  public static void setCopybookContent(Job job, String content) {
    job.getConfiguration().set(cblContent, content);
  }

  /**
   * Sets the decider key for associating records.
   *
   * @param job instance of <code>Job</code>.
   * @param deciderKey to be used for selecting records.
   */
  public static void setDeciderKey(Job job, String deciderKey) {
    job.getConfiguration().set(cblDeciderKey, deciderKey);
  }

  /**
   * Sets the input file path to be processed.
   *
   * @param job instance of <code>Job</code>.
   * @param binaryFilePath specifies the path to the input file or directory.
   */
  public static void setBinaryFilePath(Job job, String binaryFilePath) {
    job.getConfiguration().set(cblBinaryFilePath, binaryFilePath);
  }

  /**
   * Sets the selectors for record.
   *
   * @param job instance of <code>Job</code>.
   * @param selectors to be used for assigning records based on the values of <code>deciderKey</code>.
   */
  public static void setSelectors(Job job, String selectors) {
    job.getConfiguration().set(cblSelector, selectors);
  }

  /**
   * Sets whether file has to be parsed as fixed length or variable length.
   *
   * @param job instance of <code>Job</code>.
   * @param fileStructure of the file being processed.
   */
  public static void setFileStructure(Job job, String fileStructure) {
    job.getConfiguration().set(cblFileStructure, fileStructure);
  }

  /**
   * Creates <code>MainFrameRecordReader</code> configuring it.
   *
   * @param split for which the record reader is created.
   * @param context task attempt context.
   * @return a instance of <code>RecordReader</code>
   */
  @Override
  public RecordReader<LongWritable, MainframeRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    // Configure the <code>ConfigProvider</code> passing font and layout name.
    ConfigProvider.Builder configProvider = new ConfigProvider.Builder("", conf.get(cblFont, "cp307"));
    // Entire content of cobol copybook.
    String copybook = conf.get(cblContent);
    if (copybook != null && !copybook.trim().isEmpty()) {
      configProvider.setCopybookContent(conf.get(cblContent));
    } else {
      throw new IOException("COBOL copybook is not provided. Provide complete copybook");
    }
    configProvider.setBinaryFilePath(conf.get(cblBinaryFilePath));

    // Decider key defines the primary field that <code>JRecord<code> would split on.
    // The selectors define the value for primary key and the record that it is associated
    // with the value of primary key matches. The format is specified as follow
    // (condition:record[;condition:record]*)
    String deciderKey = conf.get(cblDeciderKey, null);
    String selectors = conf.get(cblSelector, null);
    if (deciderKey != null && selectors != null) {
      configProvider.setDeciderField(deciderKey);
      String[] selectorParts = selectors.split(",");
      for (String selectorPart : selectorParts) {
        String[] parts = selectorPart.trim().split(":");
        configProvider.setRecordCondition(parts[0].trim(), parts[1].trim());
      }
    }
    // Specifies the structure of the binary file as VB or Fixed.
    String structure = conf.get(cblFileStructure);
    if (structure.equalsIgnoreCase("fixed")) {
      return new MainframeRecordReader(new FixedLengthReader(), configProvider.build());
    } else {
      throw new IOException("Only 'fixed' length files supported");
    }
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    Configuration conf = context.getConfiguration();
    Path path = new Path(conf.get(cblBinaryFilePath));
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
    return (null == codec) || codec instanceof SplittableCompressionCodec;
  }
}
