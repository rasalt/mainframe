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

package io.cdap.plugin.mainframe.format;

import io.cdap.cdap.api.data.format.StructuredRecord;

/**
 * This class <code>MainframeRecord</code> is the record that is returned by the record reader.
 */
public final class MainframeRecord {
  private final StructuredRecord record;
  private final boolean error;
  private final String message;

  public MainframeRecord(StructuredRecord record) {
    this.record = record;
    this.error = false;
    this.message = "";
  }

  public MainframeRecord(String message) {
    this.error = true;
    this.message = message;
    this.record = null;
  }

  /**
   * @return true if there was error reading record from mainframe, false otherwise.
   */
  public boolean hasError() {
    return error;
  }

  /**
   * @return message representing the error reading the record.
   */
  public String getErrorMessage() {
    return message;
  }

  /**
   * @return <code>StructuredRecord</code> containing actual record read from data file.
   */
  public StructuredRecord getRecord() {
    return record;
  }
}
