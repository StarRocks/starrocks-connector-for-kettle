package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;

public class StarRocksCsvSerializer implements StarRocksISerializer{

    private final String columnSeparator;

    public StarRocksCsvSerializer(String sp){
        this.columnSeparator=null==sp?"\t":sp;
    }
    @Override
    public String serialize(Object[] values) {
        StringBuilder sb=new StringBuilder();
        int idx=0;
        for (Object val:values){
            sb.append(null==val?"\\N":((val instanceof Map || val instanceof List) ? JSON.toJSONString(val) : val));
            if (idx++< values.length-1){
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
