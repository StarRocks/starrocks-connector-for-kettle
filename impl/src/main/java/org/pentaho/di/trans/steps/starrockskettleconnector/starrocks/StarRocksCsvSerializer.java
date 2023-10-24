package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class StarRocksCsvSerializer implements StarRocksISerializer {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCsvSerializer.class);

    private final String columnSeparator;

    public StarRocksCsvSerializer(String sp) {
        this.columnSeparator = null == sp ? "\t" : sp;
    }

    @Override
    public String serialize(Object[] values) {
        StringBuilder sb = new StringBuilder();
        ObjectMapper objectMapper = new ObjectMapper();
        String strval = "";
        int idx = 0;
        for (Object val : values) {
            if (null == val) {
                strval = "\\N";
            } else if (val instanceof Map || val instanceof List) {
                try {
                    strval = objectMapper.writeValueAsString(val);
                    LOG.debug("When csv is serialized, data of type map or list appears converted to {}", strval);
                } catch (Exception e) {
                    LOG.debug("A conversion JSON error occurred during CSV serialization :{}", e.getMessage());
                }
            } else {
                strval = val.toString();
            }
            sb.append(strval);
            if (idx++ < values.length - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
