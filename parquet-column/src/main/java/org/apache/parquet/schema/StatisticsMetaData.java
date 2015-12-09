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

package org.apache.parquet.schema;
/**
 * 存储哪些路径的叶子节点需要获取相应的统计数据
 * 如user_id.type，统计出不同用户类型的用户个数
 */
import org.apache.hadoop.conf.Configuration;
import java.util.Map;
import java.util.HashMap;

public class StatisticsMetaData {
    private static Map<String, Boolean> paths_stats;

    public StatisticsMetaData(Configuration conf) {
        paths_stats = new HashMap<String, Boolean>();
        String [] paths = conf.getStrings("stats_paths");
        if (paths != null) {
            for (int ix = 0; ix < paths.length; ix++) {
                paths_stats.put(paths[ix], new Boolean(true));
            }
        }
    }

    public static boolean getEnableStatsBaseOnPaths(String []paths) {
        StringBuilder sb = new StringBuilder();
        for (int ix = 0; ix < paths.length - 1; ix++) {
            sb.append(paths[ix]).append(".");
        }
        sb.append(paths[paths.length - 1]);
        String key = sb.toString();
        Boolean value = paths_stats.get(key);
        if (value != null) {
            return value;
        } else {
            return false;
        }
    }
}