package io.cdap.plugin.mainframe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
