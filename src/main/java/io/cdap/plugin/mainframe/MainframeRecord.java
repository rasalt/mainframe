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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class <code>MainframeRecord</code>.
 */
public final class MainframeRecord {
  private Map<String, Datum> datumMap = new LinkedHashMap<>();
  private long length;
  private String name;

  private MainframeRecord(String name, long length, List<Datum> datums) {
    this.name = name;
    this.length = length;
    for (Datum datum : datums) {
      datumMap.put(datum.getName(), datum);
    }
  }

  public String getName() {
    return name;
  }

  public long getLength() {
    return length;
  }

  public Collection<Datum> getCollection() {
    return datumMap.values();
  }

  public Datum get(String name) {
    return datumMap.get(name);
  }

  public int size() {
    return datumMap.size();
  }

  /**
   *
   */
  public static class Builder {
    private String name;
    private List<Datum> datums;
    private long length;

    public Builder() {
      this.datums = new ArrayList<>();
      this.length = 0;
    }

    public void setRecordName(String name) {
      this.name = name;
    }

    public void setLength(long length) {
      this.length = length;
    }

    public void add(Datum datum) {
      this.datums.add(datum);
    }

    public void add(List<Datum> datums) {
      this.datums.addAll(datums);
    }

    public MainframeRecord build() {
      return new MainframeRecord(name, length, datums);
    }
  }
}
