/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksJsonSerializer implements StarRocksISerializer {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJsonSerializer.class);
    private final String[] fieldNames;
    private ObjectMapper objectMapper=new ObjectMapper();

    public StarRocksJsonSerializer(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String serialize(Object[] values) {
        Map<String, Object> rowMap = new LinkedHashMap<>(values.length);
        int idx = 0;
        for (String fieldName : fieldNames) {
            rowMap.put(fieldName, values[idx]);
            idx++;
        }
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(rowMap);
            LOG.debug("the data is converted into JSON:{}", jsonString);
        } catch (Exception e) {
            LOG.debug("A conversion JSON error occurred during Json serialization:{}", e.getMessage());
        }
        return jsonString;
    }
}
