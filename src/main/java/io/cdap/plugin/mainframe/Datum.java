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

import net.sf.JRecord.Details.fieldValue.IFieldValue;

/**
 * This class <code>Datum</code>.
 */
public final class Datum {
  private IFieldValue value;
  private String name;

  public Datum(String name, IFieldValue value) {
    this.name = canonicalize(name);
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public IFieldValue getValue() {
    return value;
  }

  public static String canonicalize(String fieldName) {
    return fieldName.replace("-", "_");
  }
}
