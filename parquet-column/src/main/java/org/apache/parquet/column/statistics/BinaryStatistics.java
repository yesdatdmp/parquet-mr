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

import org.apache.parquet.io.api.Binary;
import java.util.Map;
import java.util.HashMap;

public class BinaryStatistics extends Statistics<Binary> {

  private Binary max;
  private Binary min;

  @Override
  public void updateStats(Binary value) {
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
    BinaryStatistics binaryStats = (BinaryStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(binaryStats.getMin(), binaryStats.getMax());
    } else {
      updateStats(binaryStats.getMin(), binaryStats.getMax());
    }
    if (enableStats && stats.getEnableStats()) {
      Map<Binary, Long> st = stats.getStats();
      for (Map.Entry<Binary, Long> v : st.entrySet()) {
        Binary key = v.getKey();
        Long value = v.getValue();
        if (value_count.containsKey(key)) {
          value_count.put(key, value_count.get(key) + value);
        } else {
          value_count.put(key, value);
        }
      }
    }
  }

  /**
   * Sets min and max values, re-uses the byte[] passed in.
   * Any changes made to byte[] will be reflected in min and max values as well.
   * @param minBytes byte array to set the min value to
   * @param maxBytes byte array to set the max value to
   */
  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Binary.fromReusedByteArray(maxBytes);
    min = Binary.fromReusedByteArray(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return max == null ? null : max.getBytes();
  }

  @Override
  public byte[] getMinBytes() {
    return min == null ? null : min.getBytes();
  }

  @Override
  public String toString() {
    if (this.hasNonNullValue())
      return String.format("min: %s, max: %s, num_nulls: %d", min.toStringUsingUTF8(), max.toStringUsingUTF8(), this.getNumNulls());
   else if (!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
   else
      return "no stats for this column";
  }

  public void updateStats(Binary min_value, Binary max_value) {
    if (min.compareTo(min_value) > 0) { min = min_value.copy(); }
    if (max.compareTo(max_value) < 0) { max = max_value.copy(); }
  }

  public void initializeStats(Binary min_value, Binary max_value) {
    min = min_value.copy();
    max = max_value.copy();
    this.markAsNotEmpty();
  }

  @Override
  public Binary genericGetMin() {
    return min;
  }

  @Override
  public Binary genericGetMax() {
    return max;
  }

  public Binary getMax() {
    return max;
  }

  public Binary getMin() {
    return min;
  }

  public void setMinMax(Binary min, Binary max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
