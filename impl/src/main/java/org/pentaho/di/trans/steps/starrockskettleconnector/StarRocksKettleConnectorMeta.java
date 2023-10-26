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

package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.AfterInjection;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionOptions;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionProvider;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksQueryVisitor;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.http.protocol.HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE;

@Step(id = "StarRocksKettleConnector", name = "BaseStep.TypeLongDesc.StarRocksKettleConnector",
        description = "BaseStep.TypeTooltipDesc.StarRocksKettleConnector",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
        image = "StarRocks.svg",
        documentationUrl = "https://docs.starrocks.io/zh-cn/latest/introduction/StarRocks_intro",
        i18nPackageName = "org.pentaho.di.trans.steps.starrockskettleconnector")
@InjectionSupported(localizationPrefix = "StarRocksKettleConnector.Injection.", groups = {"FIELDS"})
public class StarRocksKettleConnectorMeta extends BaseStepMeta implements StarRocksMeta {

    private static Class<?> PKG = StarRocksKettleConnectorMeta.class; // for i18n purposes, needed by Translator2!!

    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final int WRITE_IO_THREAD_COUNT = 2;
    private static final int WAIT_FOR_CONTINUE_TIMEOUT = 30000;


    /**
     * Url of the stream load, if you don't specify the http/https prefix, the default http. like: `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`.
     */
    @Injection(name = "Http_URL")
    private List<String> httpurl;

    /**
     * Url of the jdbc like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.
     */
    @Injection(name = "JDBC_URL")
    private String jdbcurl;
    /**
     * Query the Starrocks field information
     */
    private StarRocksQueryVisitor starRocksQueryVisitor;

    /**
     * Database name of the stream load.
     */
    @Injection(name = "DATABASE_NAME")
    private String databasename;

    /**
     * Table name of the stream load.
     */
    @Injection(name = "TABLE_NAME")
    private String tablename;

    /**
     * StarRocks user name.
     */
    @Injection(name = "USER")
    private String user;

    /**
     * StarRocks user password.
     */
    @Injection(name = "PASSWORD")
    private String password;

    /**
     * The format of the data to be loaded. The value can be CSV or JSON.
     * The default is CSV.
     */
    @Injection(name = "FORMAT")
    private String format;
    /**
     * The column separator in CSV format.
     */
    @Injection(name = "COLUMN_SEPARATOR")
    private String column_separator;
    /**
     * The json path in JSON format.
     */
    @Injection(name = "jsonpaths")
    private String jsonpaths;

    /**
     * The maximum size of data that can be loaded into StarRocks at a time.
     * Valid values: 64 MB to 10 GB.
     */
    @Injection(name = "MAXBYTES")
    private long maxbytes;

    /**
     * The maximum fault tolerance rate for the import job.
     * Specifies the maximum fault tolerance rate for the import job, which is the maximum
     * proportion of data rows that the import job can tolerate filtered out due to substandard data quality.
     * Value range: 0~1. Default value: 0.
     */
    @Injection(name = "MAXFILTERRATIO")
    private float max_filter_ratio;

    /**
     * Timeout period for connecting to the load-url.
     * Valid values: 100 to 60000
     */
    @Injection(name = "CONNECT_TIMEOUT")
    private int connecttimeout;

    /**
     * Stream Load timeout period, in seconds.
     */
    @Injection(name = "TIMEOUT")
    private int timeout;

    /**
     * Field name of the target table
     */
    @Injection(name = "FIELD_TABLE", group = "FIELDS")
    private String[] fieldTable;

    /**
     * Field name in the stream
     */
    @Injection(name = "FIELD_STREAM", group = "FIELDS")
    private String[] fieldStream;
    /**
     * Whether to implement partial updates.
     */
    private boolean partialupdate;
    /**
     * Update those columns
     */
    private String[] partialcolumns;
    /**
     * Whether to update and delete data.
     */
    private boolean enableupsertdelete;
    /**
     * Whether to implement update insert or delete when the enableupsertdelete function is enabled.
     */
    private String upsertordelete;
    /**
     * The frequency of Stream load writes.
     */
    private long scanningFrequency;
    /**
     * The Stream Load properties.
     */
    private String headerProperties;

    /**
     * @param httpurl Url of the stream load.
     */
    public void setHttpurl(List<String> httpurl) {
        this.httpurl = httpurl;
    }

    /**
     * @return Return the http url.
     */
    public List<String> getHttpurl() {
        return this.httpurl;
    }

    /**
     * @return Return the JDBC url.
     */
    public String getJdbcurl() {
        return jdbcurl;
    }

    /**
     * @param jdbcurl Url of the jdbc.
     */
    public void setJdbcurl(String jdbcurl) {
        this.jdbcurl = jdbcurl;
    }

    /**
     * @return Return the Starrocks visitor
     */
    public StarRocksQueryVisitor getStarRocksQueryVisitor() {
        return starRocksQueryVisitor;
    }

    /**
     * @param starRocksQueryVisitor The Starrocks visitor:Used to find field information in Starrocks.
     */
    public void setStarRocksQueryVisitor(StarRocksQueryVisitor starRocksQueryVisitor) {
        this.starRocksQueryVisitor = starRocksQueryVisitor;
    }

    /**
     * @return Return the data base name.
     */
    public String getDatabasename() {
        return databasename;
    }

    /**
     * @param databasename The target data base name.
     */
    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    /**
     * @return Return the table name.
     */
    public String getTablename() {
        return tablename;
    }

    /**
     * @param tablename The target table name.
     */
    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    /**
     * @return Return the StarRocks user.
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user The StarRocks user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return Returns the password for the user.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password The password for the user.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the imported data format.
     */
    public String getFormat() {
        return format;
    }

    /**
     * @param format The imported data format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * @return The column separator in CSV format.
     */
    public String getColumnSeparator() {
        return this.column_separator;
    }

    /**
     * @param column_separator Column separator in CSV format
     */
    public void setColumnSeparator(String column_separator) {
        this.column_separator = column_separator;
    }

    /**
     * @return The Json path.
     */
    public String getJsonpaths() {
        return this.jsonpaths;
    }

    /**
     * @param jsonpaths The Json path.
     */
    public void setJsonpaths(String jsonpaths) {
        this.jsonpaths = jsonpaths;
    }

    /**
     * @return Return the maximum size of data that can be loaded into StarRocks at a time.
     */
    public long getMaxbytes() {
        return maxbytes;
    }

    /**
     * @param maxbytes The maximum size of data that can be loaded into StarRocks at a time.
     */
    public void setMaxbytes(long maxbytes) {
        this.maxbytes = maxbytes;
    }

    /**
     * @return Return the maximum fault tolerance rate for the import job.
     */
    public float getMaxFilterRatio() {
        if (max_filter_ratio > 1 || max_filter_ratio < 0) {
            return 0;
        }
        return max_filter_ratio;
    }

    /**
     * @param max_filter_ratio The maximum fault tolerance rate for the import job.
     */
    public void setMaxFilterRatio(float max_filter_ratio) {
        this.max_filter_ratio = max_filter_ratio;
    }

    /**
     * @return Return timeout period for connecting to the load-url.
     */
    public int getConnecttimeout() {
        if (connecttimeout < 100) {
            return 100;
        }
        return Math.min(connecttimeout, 60000);
    }

    /**
     * @param connecttimeout Timeout period for connecting to the load-url.
     */
    public void setConnecttimeout(int connecttimeout) {
        this.connecttimeout = connecttimeout;
    }

    /**
     * @return Return Stream Load timeout period.
     */
    public int getTimeout() {
        if (timeout < 1) {
            return 600;
        }
        return Math.min(timeout, 259200);
    }

    /**
     * @param timeout Stream Load timeout period.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Returns the fieldTable.
     */
    public String[] getFieldTable() {
        return fieldTable;
    }

    /**
     * @param fieldTable The fieldTable to set.
     */
    public void setFieldTable(String[] fieldTable) {
        this.fieldTable = fieldTable;
    }

    /**
     * @return Returns the fieldStream.
     */
    public String[] getFieldStream() {
        return fieldStream;
    }

    /**
     * @param fieldStream The fieldStream to set.
     */
    public void setFieldStream(String[] fieldStream) {
        this.fieldStream = fieldStream;
    }

    /**
     * @return Return the column that needs to be updated.
     */
    public String[] getPartialcolumns() {
        return this.partialcolumns;
    }

    /**
     * @param partialcolumns The column that needs to be updated.
     */
    public void setPartialcolumns(String[] partialcolumns) {
        this.partialcolumns = partialcolumns;
    }

    /**
     * @return Returns whether a partial update was made.
     */
    public boolean getPartialUpdate() {
        return this.partialupdate;
    }

    /**
     * @param partialupdate Whether a partial update was made.
     */
    public void setPartialupdate(boolean partialupdate) {
        this.partialupdate = partialupdate;
    }

    /**
     * @return Return whether to update and delete data.
     */
    public boolean getEnableUpsertDelete() {
        return this.enableupsertdelete;
    }

    /**
     * @param enableupsertdelete Whether to update and delete data.
     */
    public void setEnableupsertdelete(boolean enableupsertdelete) {
        this.enableupsertdelete = enableupsertdelete;
    }

    /**
     * @return Return the option of the StarRocks
     */
    public String getUpsertOrDelete() {
        return upsertordelete;
    }

    /**
     * @param upsertOrDelete The option of the StarRocks,UPSERT or DELETE.
     */
    public void setUpsertOrDelete(String upsertOrDelete) {
        this.upsertordelete = upsertOrDelete;
    }

    /**
     * @return return the frequency of Stream load writes.
     */
    public long getScanningFrequency() {
        if (this.scanningFrequency < 50) {
            return 50L;
        }
        return this.scanningFrequency;
    }

    /**
     * @param scanningFrequency The frequency of Stream load writes.
     */
    public void setScanningFrequency(long scanningFrequency) {
        this.scanningFrequency = scanningFrequency;
    }

    /**
     * @return Data chunk size in a http request for stream load.
     */
    public long getChunkLimit() {
        return 3 * GIGA_BYTES_SCALE;
    }

    /**
     * @return Stream load thread count.
     */
    public int getIoThreadCount() {
        return WRITE_IO_THREAD_COUNT;
    }

    /**
     * @return Timeout in millisecond to wait for 100-continue response for http client.
     */
    public int getWaitForContinueTimeout() {
        int waitForContinueTimeoutMs = WAIT_FOR_CONTINUE_TIMEOUT;
        if (waitForContinueTimeoutMs < DEFAULT_WAIT_FOR_CONTINUE) {
            return DEFAULT_WAIT_FOR_CONTINUE;
        }
        return Math.min(waitForContinueTimeoutMs, 600000);
    }

    /**
     * @param headerProperties Stream Load properties.
     */
    public void setHeaderProperties(String headerProperties) {
        this.headerProperties = headerProperties;
    }

    public String getHeaderProperties() {
        return this.headerProperties;
    }

    public void setDefault() {
        fieldTable = null;
        httpurl = null;
        jdbcurl = null;
        starRocksQueryVisitor = null;
        databasename = "";
        tablename = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.DefaultTableName");
        user = "root";
        password = "";
        format = "CSV";
        column_separator = "\t";
        jsonpaths = null;
        maxbytes = 90L * MEGA_BYTES_SCALE;
        max_filter_ratio = 0;
        connecttimeout = 1000;
        timeout = 600;
        partialupdate = false;
        partialcolumns = null;
        enableupsertdelete = false;
        upsertordelete = "";
        scanningFrequency = 50L;
        headerProperties = "";

        allocate(0);
    }

    public void allocate(int nrvalues) {
        fieldTable = new String[nrvalues];
        fieldStream = new String[nrvalues];
    }

    public Object clone() {
        StarRocksKettleConnectorMeta retval = (StarRocksKettleConnectorMeta) super.clone();
        int nrvalues = fieldTable.length;
        retval.allocate(nrvalues);
        System.arraycopy(fieldTable, 0, retval.fieldTable, 0, nrvalues);
        System.arraycopy(fieldStream, 0, retval.fieldStream, 0, nrvalues);

        return retval;
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            String httpurl1 = XMLHandler.getTagValue(stepnode, "httpurl");
            if (httpurl1 != null && httpurl1.length() != 0) {
                httpurl = Arrays.asList(httpurl1.split(";"));
            }
            jdbcurl = XMLHandler.getTagValue(stepnode, "jdbcurl");
            databasename = XMLHandler.getTagValue(stepnode, "databasename");
            tablename = XMLHandler.getTagValue(stepnode, "tablename");
            user = XMLHandler.getTagValue(stepnode, "user");
            password = XMLHandler.getTagValue(stepnode, "password");
            if (password == null) {
                password = "";
            }
            format = XMLHandler.getTagValue(stepnode, "format");
            column_separator = XMLHandler.getTagValue(stepnode, "columnseparator");
            if (column_separator == null) {
                column_separator = "\r";
            }
            jsonpaths = XMLHandler.getTagValue(stepnode, "jsonpaths");
            maxbytes = Long.valueOf(XMLHandler.getTagValue(stepnode, "maxbytes"));
            max_filter_ratio = Float.valueOf(XMLHandler.getTagValue(stepnode, "maxfilterratio"));
            connecttimeout = Integer.valueOf(XMLHandler.getTagValue(stepnode, "connecttimeout"));
            timeout = Integer.valueOf(XMLHandler.getTagValue(stepnode, "timeout"));
            String partialcolumns1 = XMLHandler.getTagValue(stepnode, "partialcolumns");
            if (partialcolumns1 != null && partialcolumns1.length() != 0) {
                partialcolumns = partialcolumns1.split(",");
            }

            partialupdate = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "partialupdate"));
            enableupsertdelete = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "enableupsertdelete"));

            upsertordelete = XMLHandler.getTagValue(stepnode, "upsertordelete");
            scanningFrequency = Long.valueOf(XMLHandler.getTagValue(stepnode, "scanningFrequency"));
            headerProperties = XMLHandler.getTagValue(stepnode, "headerProperties");
            // Field data mapping
            int nrvalues = XMLHandler.countNodes(stepnode, "mapping");
            allocate(nrvalues);

            for (int i = 0; i < nrvalues; i++) {
                Node vnode = XMLHandler.getSubNodeByNr(stepnode, "mapping", i);

                fieldTable[i] = XMLHandler.getTagValue(vnode, "stream_name");
                fieldStream[i] = XMLHandler.getTagValue(vnode, "field_name");
                if (fieldStream[i] == null) {
                    fieldStream[i] = fieldTable[i]; // default: the same name!
                }
            }

        } catch (Exception e) {
            throw new KettleXMLException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnableToReadStepInfoFromXML"), e);
        }

    }

    public String getXML() {
        StringBuilder retval = new StringBuilder(300);

        String httpurl1 = "";
        if (httpurl != null && httpurl.size() != 0) {
            httpurl1 = String.join(";", httpurl);
        }
        retval.append("    ").append(XMLHandler.addTagValue("httpurl", httpurl1));
        retval.append("    ").append(XMLHandler.addTagValue("jdbcurl", jdbcurl));
        retval.append("    ").append(XMLHandler.addTagValue("databasename", databasename));
        retval.append("    ").append(XMLHandler.addTagValue("tablename", tablename));
        retval.append("    ").append(XMLHandler.addTagValue("user", user));
        retval.append("    ").append(XMLHandler.addTagValue("password", password));
        retval.append("    ").append(XMLHandler.addTagValue("format", format));
        retval.append("    ").append(XMLHandler.addTagValue("columnseparator", column_separator));
        retval.append("    ").append(XMLHandler.addTagValue("jsonpaths", jsonpaths));
        retval.append("    ").append(XMLHandler.addTagValue("maxbytes", maxbytes));
        retval.append("    ").append(XMLHandler.addTagValue("maxfilterratio", max_filter_ratio));
        retval.append("    ").append(XMLHandler.addTagValue("connecttimeout", connecttimeout));
        retval.append("    ").append(XMLHandler.addTagValue("timeout", timeout));
        retval.append("    ").append(XMLHandler.addTagValue("partialupdate", partialupdate));
        String partialcolumns1 = "";
        if (partialcolumns != null && partialcolumns.length != 0) {
            partialcolumns1 = String.join(",", partialcolumns);
        }
        retval.append("    ").append(XMLHandler.addTagValue("partialcolumns", partialcolumns1));
        retval.append("    ").append(XMLHandler.addTagValue("enableupsertdelete", enableupsertdelete));
        retval.append("    ").append(XMLHandler.addTagValue("upsertordelete", upsertordelete));
        retval.append("    ").append(XMLHandler.addTagValue("scanningFrequency", scanningFrequency));
        retval.append("    ").append(XMLHandler.addTagValue("headerProperties", headerProperties));

        for (int i = 0; i < fieldTable.length; i++) {
            retval.append("      <mapping>").append(Const.CR);
            retval.append("        ").append(XMLHandler.addTagValue("stream_name", fieldTable[i]));
            retval.append("        ").append(XMLHandler.addTagValue("field_name", fieldStream[i]));
            retval.append("      </mapping>").append(Const.CR);
        }

        return retval.toString();
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        try {
            String httpurl1 = rep.getStepAttributeString(id_step, "httpurl");
            if (httpurl1 != null && httpurl1.length() != 0) {
                httpurl = Arrays.asList(httpurl1.split(";"));
            }
            jdbcurl = rep.getStepAttributeString(id_step, "jdbcurl");
            databasename = rep.getStepAttributeString(id_step, "databasename");
            tablename = rep.getStepAttributeString(id_step, "tablename");
            user = rep.getStepAttributeString(id_step, "user");
            password = rep.getStepAttributeString(id_step, "password");
            if (password == null) {
                password = "";
            }
            format = rep.getStepAttributeString(id_step, "format");
            column_separator = rep.getStepAttributeString(id_step, "columnseparator");
            if (column_separator == null) {
                column_separator = "\r";
            }
            jsonpaths = rep.getStepAttributeString(id_step, "jsonpaths");
            maxbytes = Long.valueOf(rep.getStepAttributeString(id_step, "maxbytes"));
            max_filter_ratio = Float.valueOf(rep.getStepAttributeString(id_step, "maxfilterratio"));
            connecttimeout = (int) rep.getStepAttributeInteger(id_step, "connecttimeout");
            timeout = (int) rep.getStepAttributeInteger(id_step, "timeout");
            partialupdate = rep.getStepAttributeBoolean(id_step, "partialupdate");
            String partialcolumns1 = rep.getStepAttributeString(id_step, "partialcolumns");
            if (partialcolumns1 != null && partialcolumns1.length() != 0) {
                partialcolumns = partialcolumns1.split(",");
            }
            enableupsertdelete = rep.getStepAttributeBoolean(id_step, "enableupsertdelete");
            upsertordelete = rep.getStepAttributeString(id_step, "upsertordelete");
            scanningFrequency = Long.valueOf(rep.getStepAttributeString(id_step, "scanningFrequency"));
            headerProperties = rep.getStepAttributeString(id_step, "headerProperties");
            int nrvalues = rep.countNrStepAttributes(id_step, "stream_name");

            allocate(nrvalues);

            for (int i = 0; i < nrvalues; i++) {
                fieldTable[i] = rep.getStepAttributeString(id_step, i, "stream_name");
                fieldStream[i] = rep.getStepAttributeString(id_step, i, "field_name");
                if (fieldStream[i] == null) {
                    fieldStream[i] = fieldTable[i];
                }
            }

        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository"), e);
        }
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            String httpurl1 = "";
            if (httpurl != null && httpurl.size() != 0) {
                httpurl1 = String.join(";", httpurl);
            }
            rep.saveStepAttribute(id_transformation, id_step, "httpurl", httpurl1);
            rep.saveStepAttribute(id_transformation, id_step, "jdbcurl", jdbcurl);
            rep.saveStepAttribute(id_transformation, id_step, "databasename", databasename);
            rep.saveStepAttribute(id_transformation, id_step, "tablename", tablename);
            rep.saveStepAttribute(id_transformation, id_step, "user", user);
            rep.saveStepAttribute(id_transformation, id_step, "password", password);
            rep.saveStepAttribute(id_transformation, id_step, "format", format);
            rep.saveStepAttribute(id_transformation, id_step, "columnseparator", column_separator);
            rep.saveStepAttribute(id_transformation, id_step, "jsonpaths", jsonpaths);
            rep.saveStepAttribute(id_transformation, id_step, "maxbytes", maxbytes);
            rep.saveStepAttribute(id_transformation, id_step, "maxfilterratio", max_filter_ratio);
            rep.saveStepAttribute(id_transformation, id_step, "connecttimeout", connecttimeout);
            rep.saveStepAttribute(id_transformation, id_step, "timeout", timeout);
            rep.saveStepAttribute(id_transformation, id_step, "partialupdate", partialupdate);
            String partialcolumns1 = "";
            if (partialcolumns != null && partialcolumns.length != 0) {
                partialcolumns1 = String.join(",", partialcolumns);
            }
            rep.saveStepAttribute(id_transformation, id_step, "partialcolumns", partialcolumns1);
            rep.saveStepAttribute(id_transformation, id_step, "enableupsertdelete", enableupsertdelete);
            rep.saveStepAttribute(id_transformation, id_step, "upsertordelete", upsertordelete);
            rep.saveStepAttribute(id_transformation, id_step, "scanningFrequency", scanningFrequency);
            rep.saveStepAttribute(id_transformation, id_step, "headerProperties", headerProperties);

            for (int i = 0; i < fieldTable.length; i++) {
                rep.saveStepAttribute(id_transformation, id_step, i, "stream_name", fieldTable[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldStream[i]);
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnableToSaveStepInfoToRepository") + id_step, e);
        }
    }

    public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
        // Default: nothing changes to rowMeta
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore) {
        CheckResult cr;
        String error_message = "";

        if (jdbcurl != null) {
            try {
                if (starRocksQueryVisitor == null) {
                    // Used to find field information in Starrocks.
                    StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(this.jdbcurl, this.user, this.password);
                    StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                    starRocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnectionProvider, this.databasename, this.tablename);
                }

                // Verify that the table exists.
                if (!Utils.isEmpty(tablename)) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "StarRocksKettleConnectorMeta.CheckResult.TableNameOK"), stepMeta);
                    remarks.add(cr);
                    try {
                        if (!starRocksQueryVisitor.getAllTables().contains(this.tablename)) {
                            error_message = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.NoNeedTable") + tablename;
                            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                            remarks.add(cr);
                        }
                    } catch (Exception e) {
                        error_message = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.ErrorConnJDBC") + e.getMessage();
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                        remarks.add(cr);
                    }
                }

                // Check fields in table
                boolean first = true;
                boolean error_found = false;
                error_message = "";

                Map<String, StarRocksDataType> fielsMap = starRocksQueryVisitor.getFieldMapping();
                if (fielsMap != null) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.TableExists"), stepMeta);
                    remarks.add(cr);

                    // How about the fields to insert/dateMask in the table?
                    first = true;
                    error_found = false;
                    error_message = "";

                    for (int i = 0; i < fieldTable.length; i++) {
                        String field = fieldTable[i];

                        boolean isFieldExists = fielsMap.containsKey(field);
                        if (!isFieldExists) {
                            if (first) {
                                first = false;
                                error_message += BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.MissingFieldsToLoadInTargetTable") + Const.CR;
                            }
                            error_found = true;
                            error_message += "\t\t" + field + Const.CR;
                        }

                    }
                    if (error_found) {
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                    } else {
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.AllFieldsFoundInTargetTable"), stepMeta);
                    }
                    remarks.add(cr);
                }

                // Look up fields in the input stream <prev>
                if (prev != null && prev.size() > 0) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.StepReceivingDatas", prev.size() + ""), stepMeta);
                    remarks.add(cr);

                    first = true;
                    error_found = false;
                    error_message = "";

                    for (int i = 0; i < fieldStream.length; i++) {
                        ValueMetaInterface v = prev.searchValueMeta(fieldStream[i]);
                        if (v == null) {
                            if (first) {
                                first = false;
                                error_message +=
                                        BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.MissingFieldsInInput") + Const.CR;
                            }
                            error_found = true;
                            error_message += "\t\t" + fieldStream[i] + Const.CR;
                        }
                    }
                    if (error_found) {
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                    } else {
                        cr =
                                new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                                        "StarRocksKettleConnectorMeta.CheckResult.AllFieldsFoundInInput"), stepMeta);
                    }
                    remarks.add(cr);
                } else {
                    error_message = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.MissingFieldsInInput3") + Const.CR;
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                    remarks.add(cr);
                }

                // Look up partial columns in the field name.
                if (partialupdate) {
                    error_found = false;
                    first = true;
                    error_message = "";
                    for (String field : partialcolumns) {
                        if (!containsString(fieldTable, field)) {
                            if (first) {
                                first = false;
                                error_message += BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.MissingPartialColumns") + Const.CR;
                            }
                            error_found = true;
                            error_message += "\t\t" + field + Const.CR;
                        }
                    }
                    if (error_found) {
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                    } else {
                        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.AllColumnsFoundInFieldTable"), stepMeta);
                    }
                    remarks.add(cr);
                } else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.NoNeedPartialUpdate"), stepMeta);
                    remarks.add(cr);
                }

                // Check the Kettle-StarRocks Type Mapping.
                error_found = false;
                first = true;
                error_message = "";
                for (int i = 0; i < fieldStream.length; i++) {
                    ValueMetaInterface v = prev.searchValueMeta(fieldStream[i]);
                    StarRocksDataType type = fielsMap.get(fieldTable[i]);
                    if (!isCorrectTypeMapping(v.getType(), type)) {
                        if (first) {
                            first = false;
                            error_message += BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.ErrorTypeMapping") + Const.CR;
                        }
                        error_found = true;
                        error_message += "\t\t" + ValueMetaInterface.getTypeDescription(v.getType()) + "---->" + type.toString() + Const.CR;
                    }
                }
                if (error_found) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                } else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.CorrectTypeMapping"), stepMeta);
                }
                remarks.add(cr);

            } catch (Exception e) {
                error_message = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.DatabaseErrorOccurred") + e.getMessage();
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
                remarks.add(cr);
            }
        } else {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.NoJDBCUrl"), stepMeta);
            remarks.add(cr);
        }


        // See if we have input streams leading to this step!
        if (input.length > 0) {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "StarRocksKettleConnectorMeta.CheckResult.StepReceivingInfoFromOtherSteps"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                            "StarRocksKettleConnectorMeta.CheckResult.NoInputError"), stepMeta);
            remarks.add(cr);
        }
        // Check the Format
        if (format == "CSV") {
            if (column_separator == null || column_separator.length() == 0) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.NoSeparator"), stepMeta);
                remarks.add(cr);
            }
        } else if (format == "JSON") {
            if (jsonpaths == null || jsonpaths.length() == 0) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.CheckResult.NoJsonpaths"), stepMeta);
                remarks.add(cr);
            }
        }

    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                 Trans trans) {
        return new StarRocksKettleConnector(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new StarRocksKettleConnectorData();
    }

    public boolean containsString(String[] array, String a) {
        for (String element : array) {
            if (element.equals(a)) {
                return true;
            }
        }
        return false;
    }

    public boolean isOpAutoProjectionInJson() {
        String version = getStarRocksQueryVisitor().getStarRocksVersion();
        return version == null || version.length() > 0 && !version.trim().startsWith("1.");
    }

    private boolean isCorrectTypeMapping(int kettleType, StarRocksDataType starrocksType) {
        if (starrocksType == null) {
            return false;
        }
        switch (kettleType) {
            case ValueMetaInterface.TYPE_NUMBER:
            case ValueMetaInterface.TYPE_STRING:
            case ValueMetaInterface.TYPE_DATE:
            case ValueMetaInterface.TYPE_BOOLEAN:
            case ValueMetaInterface.TYPE_INTEGER:
            case ValueMetaInterface.TYPE_BIGNUMBER:
            case ValueMetaInterface.TYPE_TIMESTAMP:
            case ValueMetaInterface.TYPE_INET:
                if (typeMapping.get(kettleType).contains(starrocksType)) {
                    return true;
                }
            case ValueMetaInterface.TYPE_BINARY:
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportBinary"));
            case ValueMetaInterface.TYPE_SERIALIZABLE:
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportSerializable"));
            default:
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnknowType"));
        }
        return false;
    }

    /**
     * If we use injection we can have different arrays lengths.
     * We need synchronize them for consistency behavior with UI
     */
    @AfterInjection
    public void afterInjectionSynchronization() {
        int nrFields = (fieldTable == null) ? -1 : fieldTable.length;
        if (nrFields <= 0) {
            return;
        }
        String[][] rtnStrings = Utils.normalizeArrays(nrFields, fieldStream);
        fieldStream = rtnStrings[0];

    }

    private Map<Integer, List<StarRocksDataType>> typeMapping = new HashMap<Integer, List<StarRocksDataType>>() {
        {
            put(ValueMetaInterface.TYPE_NUMBER, Arrays.asList(StarRocksDataType.DOUBLE, StarRocksDataType.FLOAT));
            put(ValueMetaInterface.TYPE_STRING, Arrays.asList(StarRocksDataType.VARCHAR, StarRocksDataType.CHAR, StarRocksDataType.STRING, StarRocksDataType.JSON));
            put(ValueMetaInterface.TYPE_DATE, Arrays.asList(StarRocksDataType.DATE, StarRocksDataType.DATETIME));
            put(ValueMetaInterface.TYPE_BOOLEAN, Arrays.asList(StarRocksDataType.BOOLEAN, StarRocksDataType.TINYINT));
            put(ValueMetaInterface.TYPE_INTEGER, Arrays.asList(StarRocksDataType.TINYINT, StarRocksDataType.SMALLINT, StarRocksDataType.INT, StarRocksDataType.BIGINT));
            put(ValueMetaInterface.TYPE_BIGNUMBER, Arrays.asList(StarRocksDataType.LARGEINT, StarRocksDataType.DECIMAL,StarRocksDataType.UNKNOWN));
            put(ValueMetaInterface.TYPE_TIMESTAMP, Arrays.asList(StarRocksDataType.DATETIME, StarRocksDataType.DATE));
            put(ValueMetaInterface.TYPE_INET, Arrays.asList(StarRocksDataType.STRING));
        }
    };
}
