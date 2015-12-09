/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.statistics;

import org.apache.parquet.bytes.BytesUtils;
import java.util.Map;

public class LongStatistics extends Statistics<Long> {

  private long max;
  private long min;

  @Override
  public void updateStats(long value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
    if (enableStats) {
      Long v = value_count.get(value);
      if (v != null) {
        value_count.put(value, v + 1);
      } else {
        value_count.put(value, new Long(1));
      }
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    LongStatistics longStats = (LongStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(longStats.getMin(), longStats.getMax());
    } else {
      updateStats(longStats.getMin(), longStats.getMax());
    }
    if (enableStats && stats.getEnableStats()) {
      Map<Long, Long> st = stats.getStats();
      for (Map.Entry<Long, Long> v : st.entrySet()) {
        Long key = v.getKey();
        Long value = v.getValue();
        if (value_count.containsKey(key)) {
          value_count.put(key, value_count.get(key) + value);
        } else {
          value_count.put(key, value);
        }
      }
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToLong(maxBytes);
    min = BytesUtils.bytesToLong(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(min);
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %d, max: %d, num_nulls: %d", min, max, this.getNumNulls());
    else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

  public void updateStats(long min_value, long max_value) {
    if (min_value < min) { min = min_value; }
    if (max_value > max) { max = max_value; }
  }

  public void initializeStats(long min_value, long max_value) {
      min = min_value;
      max = max_value;
      this.markAsNotEmpty();
  }

  @Override
  public Long genericGetMin() {
    return min;
  }

  @Override
  public Long genericGetMax() {
    return max;
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }

  public void setMinMax(long min, long max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
