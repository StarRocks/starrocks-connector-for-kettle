package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksCsvSerializer;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksISerializer;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionOptions;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionProvider;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJsonSerializer;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksQueryVisitor;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksKettleConnector extends BaseStep implements StepInterface {

    private static Class<?> PKG = StarRocksKettleConnectorMeta.class;
    private StarRocksKettleConnectorMeta meta;
    private StarRocksKettleConnectorData data;
    /**
     * The desired delay time for data refresh。
     * ageThreshold = expectDelayTime / scanFrequency;
     * Calculate the latest submission time。
     */
    private long expectDelayTime = 30000L;

    public StarRocksKettleConnector(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {
            
            Object[] r = getRow(); // Get row from input rowset & set row busy!
            if (r == null) { // no more input to be expected...
                setOutputDone();
                closeOutput();
                return false;
            }

            if (data.streamLoadManager.getException() != null) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.AsyncWriteError"), data.streamLoadManager.getException());
                setErrors(1);
                stopAll();
                setOutputDone();
                return false;
            }

            if (first) {
                first = false;

                // Cache field indexes.
                data.keynrs = new int[meta.getFieldStream().length];
                for (int i = 0; i < data.keynrs.length; i++) {
                    data.keynrs[i] = getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                }
                data.serializer = getSerializer(meta);
            }
            String serializedValue = data.serializer.serialize(transform(r, meta.getEnableUpsertDelete()));
            data.streamLoadManager.write(null, data.databasename, data.tablename, serializedValue);

            putRow(getInputRowMeta(), r);
            incrementLinesOutput();
            return true;

        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.ErrorInStep") + e);
            setErrors(1);
            stopAll();
            setOutputDone();
            return false;
        }
    }

    private void closeOutput() throws Exception {
        data.streamLoadManager.flush();
        data.streamLoadManager.close();
        if (data.streamLoadManager.getException() != null) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadManager.getException());
        }
        data.streamLoadManager = null;
    }

    // Data type conversion.
    public Object[] transform(Object[] r, boolean supportUpsertDelete) throws KettleException {
        Object[] values = new Object[data.keynrs.length + (supportUpsertDelete ? 1 : 0)];
        for (int i = 0; i < data.keynrs.length; i++) {
            ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);
            StarRocksDataType dataType = data.fieldtype.get(meta.getFieldTable()[i]);
            values[i] = typeConvertion(sourceMeta, dataType, r[i]);
        }
        if (supportUpsertDelete && meta.getUpsertOrDelete() != null && meta.getUpsertOrDelete().length() != 0) {
            values[data.keynrs.length] = StarRocksOP.parse(meta.getUpsertOrDelete()).ordinal();
        }
        return values;
    }

    /**
     * Data type conversion.
     *
     * @param sourceMeta
     * @param type
     * @param r
     * @return
     */
    public Object typeConvertion(ValueMetaInterface sourceMeta, StarRocksDataType type, Object r) throws KettleException {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_STRING:
                    String sValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        sValue = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        sValue = sourceMeta.getString(r);
                    }
                    return sValue;
                case ValueMetaInterface.TYPE_BOOLEAN:
                    Boolean boolenaValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        String binaryBoolean = new String((byte[]) r, StandardCharsets.UTF_8);
                        boolenaValue = binaryBoolean.equals("1") || binaryBoolean.equals("true") || binaryBoolean.equals("True") || binaryBoolean.equals("TRUE");
                    } else {
                        boolenaValue = sourceMeta.getBoolean(r);
                    }
                    return boolenaValue;
                case ValueMetaInterface.TYPE_INTEGER:
                    Long integerValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        integerValue = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        integerValue = sourceMeta.getInteger(r);
                    }
                    if (integerValue >= Byte.MIN_VALUE && integerValue <= Byte.MAX_VALUE && type == StarRocksDataType.TINYINT) {
                        return integerValue.byteValue();
                    } else if (integerValue >= Short.MIN_VALUE && integerValue <= Short.MAX_VALUE && type == StarRocksDataType.SMALLINT) {
                        return integerValue.shortValue();
                    } else if (integerValue >= Integer.MIN_VALUE && integerValue <= Integer.MAX_VALUE && type == StarRocksDataType.INT) {
                        return integerValue.intValue();
                    } else {
                        return integerValue;
                    }
                case ValueMetaInterface.TYPE_NUMBER:
                    Double doubleValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        doubleValue = Double.parseDouble(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        doubleValue = sourceMeta.getNumber(r);
                    }
                    return doubleValue;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    BigDecimal decimalValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        decimalValue = new BigDecimal(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        decimalValue = sourceMeta.getBigNumber(r);
                    }
                    return decimalValue; // BigDecimal string representation is compatible with DECIMAL
                case ValueMetaInterface.TYPE_DATE:
                    SimpleDateFormat sourceDateFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat dateFormatter = null;
                    if (type == StarRocksDataType.DATE) {
                        // StarRocks DATE type format: 'yyyy-MM-dd'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    Date dateValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String dateStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        dateValue = sourceDateFormatter.parse(dateStr);
                    } else {
                        dateValue = sourceMeta.getDate(r);
                    }

                    return dateFormatter.format(dateValue);
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    SimpleDateFormat sourceTimestampFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat timeStampFormatter = null;
                    if (type == StarRocksDataType.DATE) {
                        // StarRocks DATE type format: 'yyyy-MM-dd'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    java.sql.Timestamp timestampValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String timestampStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        timestampValue = new java.sql.Timestamp(sourceTimestampFormatter.parse(timestampStr).getTime());
                    } else {
                        timestampValue = (Timestamp) sourceMeta.getDate(r);
                    }
                    return timeStampFormatter.format(timestampValue);
                case ValueMetaInterface.TYPE_BINARY:
                    throw new KettleException((BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportBinary") + r.toString()));

                case ValueMetaInterface.TYPE_INET:
                    String address;
                    if (sourceMeta.isStorageBinaryString()) {

                        address = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        address = (String) r;
                    }
                    return address;
                default:
                    throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnknowType") + ValueMetaInterface.getTypeDescription(sourceMeta.getType()));
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConvertType") + e.getMessage());
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        if (super.init(smi, sdi)) {
            // Add columns properties to all to prevent changes in the order of the fields.
            if (meta.getPartialUpdate() && meta.getPartialcolumns() != null && meta.getPartialcolumns().length != 0) {
                data.columns = new String[meta.getPartialcolumns().length];
                System.arraycopy(meta.getPartialcolumns(), 0, data.columns, 0, meta.getPartialcolumns().length);
            } else {
                data.columns = new String[meta.getFieldTable().length];
                System.arraycopy(meta.getFieldTable(), 0, data.columns, 0, meta.getFieldTable().length);
            }
            if (meta.getStarRocksQueryVisitor() == null) {
                // Used to find field information in Starrocks.
                StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(meta.getJdbcurl(), meta.getUser(), meta.getPassword());
                StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                meta.setStarRocksQueryVisitor(new StarRocksQueryVisitor(jdbcConnectionProvider, meta.getDatabasename(), meta.getTablename()));
            }
            try {
                data.streamLoadManager = new StreamLoadManagerV2(getProperties(meta, data), true);
                data.streamLoadManager.init();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConnManager"), e);
                return false;
            }
            try {
                data.fieldtype = meta.getStarRocksQueryVisitor().getFieldMapping();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.MissingStarRocksFieldType"));
                return false;
            }
            data.tablename = meta.getTablename();
            data.databasename = meta.getDatabasename();
            return true;
        }
        return false;

    }

    public StarRocksISerializer getSerializer(StarRocksKettleConnectorMeta meta) {
        StarRocksISerializer serializer;
        if (meta.getFormat().equals("CSV")) {
            serializer = new StarRocksCsvSerializer(meta.getColumnSeparator());
        } else if (meta.getFormat().equals("JSON")) {
            serializer = new StarRocksJsonSerializer(meta.getFieldTable());
        } else {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFormat"));
            return null;
        }
        return serializer;
    }

    // Get the property values needed for Stream Load loading.
    public StreamLoadProperties getProperties(StarRocksKettleConnectorMeta meta, StarRocksKettleConnectorData data) {
        StreamLoadDataFormat dataFormat;
        if (meta.getFormat().equals("CSV")) {
            dataFormat = StreamLoadDataFormat.CSV;
        } else if (meta.getFormat().equals("JSON")) {
            dataFormat = StreamLoadDataFormat.JSON;
        } else {
            throw new RuntimeException("data format are not support");
        }
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(meta.getDatabasename())
                .table(meta.getTablename())
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(meta.getChunkLimit())
                .enableUpsertDelete(meta.getEnableUpsertDelete());
        // Add the '__op' field
        if (data.columns != null) {
            // don't need to add "columns" header in following cases
            // 1. use csv format but the flink and starrocks schemas are aligned
            // 2. use json format, except that it's loading o a primary key table for StarRocks 1.x
            boolean noNeedAddColumnsHeader;
            if (dataFormat instanceof StreamLoadDataFormat.CSVFormat) {
                noNeedAddColumnsHeader = false;
            } else {
                noNeedAddColumnsHeader = !meta.getEnableUpsertDelete() || meta.isOpAutoProjectionInJson();
            }
            if (!noNeedAddColumnsHeader) {
                String[] headerColumns;
                if (meta.getEnableUpsertDelete() && meta.getUpsertOrDelete() != null && meta.getUpsertOrDelete().length() != 0) {
                    headerColumns = new String[data.columns.length + 1];
                    System.arraycopy(data.columns, 0, headerColumns, 0, data.columns.length);
                    headerColumns[data.columns.length] = "__op";
                } else {
                    headerColumns = data.columns;
                }
                String cols = Arrays.stream(headerColumns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
                defaultTablePropertiesBuilder.columns(cols);
            } else {
                String cols = Arrays.stream(data.columns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
                defaultTablePropertiesBuilder.columns(cols);
            }
        }

        Map<String, String> streamLoadProperties = new HashMap<>();
        // By default, using json format should enable strip_outer_array and ignore_json_size,
        // which will simplify the configurations
        if (dataFormat instanceof StreamLoadDataFormat.JSONFormat) {
            if (!streamLoadProperties.containsKey("strip_outer_array")) {
                streamLoadProperties.put("strip_outer_array", "true");
            }
            if (!streamLoadProperties.containsKey("ignore_json_size")) {
                streamLoadProperties.put("ignore_json_size", "true");
            }
            if (!streamLoadProperties.containsKey("format")) {
                streamLoadProperties.put("format", "json");
            }
            if (meta.getJsonpaths() != null && meta.getJsonpaths().length() != 0) {
                streamLoadProperties.put("jsonpaths", meta.getJsonpaths());
            }
        }
        if (meta.getPartialUpdate()) {
            if (!streamLoadProperties.containsKey("partial_update")) {
                streamLoadProperties.put("partial_update", "true");
            }
        }

        if (meta.getHeaderProperties() != null && meta.getHeaderProperties().length() != 0) {
            try {
                String[] properties = meta.getHeaderProperties().split(";");
                for (String property : properties) {
                    streamLoadProperties.put(property.split(":")[0], property.split(":")[0]);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnableProperties"));
            }
        }

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .labelPrefix("StarRocks-Kettle")
                .loadUrls(meta.getHttpurl().toArray(new String[0]))
                .jdbcUrl(meta.getJdbcurl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .username(meta.getUser())
                .password(meta.getPassword())
                .cacheMaxBytes(meta.getMaxbytes())
                .ioThreadCount(meta.getIoThreadCount())
                .waitForContinueTimeoutMs(meta.getWaitForContinueTimeout())
                .scanningFrequency(meta.getScanningFrequency())
                .connectTimeout(meta.getConnecttimeout())
                .version(meta.getStarRocksQueryVisitor().getStarRocksVersion())
                .maxRetries(0)
                .expectDelayTime(expectDelayTime)
                .addHeaders(streamLoadProperties)
                .addHeader("timeout", String.valueOf(meta.getTimeout()))
                .addHeader("max_filter_ratio", String.valueOf(meta.getMaxFilterRatio()));

        if (dataFormat instanceof StreamLoadDataFormat.CSVFormat) {
            builder.addHeader("column_separator", meta.getColumnSeparator());
        }

        return builder.build();

    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {
            if (data.streamLoadManager != null) {
                data.streamLoadManager.flush();
                if (data.streamLoadManager.getException() != null) {
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadManager.getException());
                }
                data.streamLoadManager.close();
                data.streamLoadManager = null;
            }
        } catch (Exception e) {
            setErrors(1L);
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UNEXPECTEDERRORCLOSING"), e);
        }
        super.dispose(smi, sdi);
    }


}
