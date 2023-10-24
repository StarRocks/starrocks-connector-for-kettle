package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksJsonSerializer implements StarRocksISerializer {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJsonSerializer.class);
    private final String[] fieldNames;

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
        ObjectMapper objectMapper = new ObjectMapper();
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
