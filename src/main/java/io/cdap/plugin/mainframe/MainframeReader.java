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

import java.io.IOException;

/**
 * This interface <code>MainframeReader</code> is base interface for retrieving records for fixed and VB files.
 */
public interface MainframeReader {
  /**
   * Initiialize the mainframe reader.
   *
   * @param provider Confiuguration provider.
   * @throws IOException thrown when there are issues reading the file.
   */
  void initialize(ConfigProvider provider) throws IOException;

  /**
   * Retrieves one record from the mainframe file.
   *
   * @return a instance of <code>Record</code>
   * @throws IOException thrown when there are issues reading records.
   */
  MainframeRecord getRecord() throws IOException;

  /**
   *
   * @throws IOException
   */
  void close() throws IOException;
}
