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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.metastore.api.IMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StarRocksKettleConnectorTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksKettleConnectorTest.class);

    StarRocksKettleConnectorMeta lmeta;
    StarRocksKettleConnectorData ldata;
    StarRocksKettleConnector lder;
    StepMeta smeta;

    @BeforeClass
    public static void initEnvironment() throws Exception {
        KettleEnvironment.init();
    }

    @Before
    public void setUp() {
        String HTTP_URLS = System.getProperty("http_urls");
        String JDBC_URLS = System.getProperty("jdbc_urls");
        assumeTrue(HTTP_URLS != null && JDBC_URLS != null);
        LOG.info("The test parameters passed in:http_urls=" + HTTP_URLS + " jdbc_urls=" + JDBC_URLS);
        String USER = System.getProperty("user");
        String PASSWORD = System.getProperty("password");
        if (USER != null) {
            LOG.info("StarRocks login information:user=" + USER + " password=" + (PASSWORD == null ? "" : PASSWORD));
        }
        TransMeta transMeta = new TransMeta();
        transMeta.setName("StarRocksKettleConnector");

        Map<String, String> vars = new HashMap<>();
        vars.put("httpurl", HTTP_URLS);
        vars.put("jdbcurl", JDBC_URLS);
        vars.put("databasename", "somedatabase");
        vars.put("tablename", "sometable");
        vars.put("user", USER == null ? "root" : USER);
        vars.put("password", PASSWORD == null ? "" : PASSWORD);
        vars.put("format", "CSV");
        transMeta.injectVariables(vars);

        lmeta = new StarRocksKettleConnectorMeta();
        List<String> httpurl = Arrays.asList(vars.get("httpurl").split(";"));
        lmeta.setHttpurl(httpurl);
        lmeta.setJdbcurl(transMeta.environmentSubstitute("${jdbcurl}"));
        lmeta.setDatabasename(transMeta.environmentSubstitute("${databasename}"));
        lmeta.setTablename(transMeta.environmentSubstitute("${tablename}"));
        lmeta.setUser(transMeta.environmentSubstitute("${user}"));
        lmeta.setPassword(transMeta.environmentSubstitute("${password}"));
        lmeta.setFormat(transMeta.environmentSubstitute("${format}"));
        lmeta.setMaxbytes(94371840);
        lmeta.setScanningFrequency(50L);
        lmeta.setConnecttimeout(1000);
        lmeta.setTimeout(600);
        lmeta.setMaxFilterRatio(0);
        lmeta.setColumnSeparator("\t");

        ldata = new StarRocksKettleConnectorData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String skcPid = plugReg.getPluginId(StepPluginType.class, lmeta);
        smeta = new StepMeta(skcPid, "StarRocksKettleConnector", lmeta);
        Trans trans = new Trans(transMeta);
        transMeta.addStep(smeta);
        lder = new StarRocksKettleConnector(smeta, ldata, 1, transMeta, trans);
        lder.copyVariablesFrom(transMeta);
    }

    @Test
    public void testGetXMLAndLoadXML() throws KettleXMLException {
        lmeta.setFieldTable(new String[0]);

        String xmlString = lmeta.getXML();

        Document document = XMLHandler.loadXMLString("<Step>" + xmlString + "</Step>");

        Node stepNode = (Node) document.getDocumentElement();

        IMetaStore metaStore = null;
        StarRocksKettleConnectorMeta newMeta = new StarRocksKettleConnectorMeta();
        newMeta.loadXML(stepNode, null, metaStore);

        assertEquals(lmeta.getHttpurl(), newMeta.getHttpurl());
        assertEquals(lmeta.getJdbcurl(), newMeta.getJdbcurl());
        assertEquals(lmeta.getDatabasename(), newMeta.getDatabasename());
        assertEquals(lmeta.getTablename(), newMeta.getTablename());
        assertEquals(lmeta.getUser(), newMeta.getUser());
        assertEquals(lmeta.getPassword(), newMeta.getPassword());
        assertEquals(lmeta.getFormat(), newMeta.getFormat());
        assertEquals(lmeta.getMaxbytes(), newMeta.getMaxbytes());
        assertEquals(lmeta.getTimeout(), newMeta.getTimeout());
        assertEquals(lmeta.getConnecttimeout(), newMeta.getConnecttimeout());
        assertEquals(lmeta.getMaxFilterRatio(), newMeta.getMaxFilterRatio(), 0.0001);
        assertEquals(lmeta.getColumnSeparator(), newMeta.getColumnSeparator());
        assertEquals(lmeta.getJsonpaths(), newMeta.getJsonpaths());
    }

    @Test
    public void testVariableSubstitution() throws KettleException {
        lmeta.setFieldTable(new String[0]);
        lder.init(lmeta, ldata);

        assertEquals("somedatabase", ldata.databasename);
        assertEquals("sometable", ldata.tablename);

    }

    @Test
    public void testTypeConversionForAllTypes() throws KettleException, Exception {

        StarRocksKettleConnector connector = lder;
        RowMeta rm = new RowMeta();
        // Test for String
        ValueMetaString vs = new ValueMetaString("string");
        rm.addValueMeta(vs);
        assertEquals("normalString", connector.typeConvertion(rm.getValueMeta(0), null, "normalString"));

        // Test for Boolean
        ValueMetaBoolean vb = new ValueMetaBoolean("boolean");
        rm.addValueMeta(vb);
        assertEquals(true, connector.typeConvertion(rm.getValueMeta(1), null, true));
        assertEquals(false, connector.typeConvertion(rm.getValueMeta(1), null, false));

        // Test for Integer
        ValueMetaInteger vi = new ValueMetaInteger("integer");
        rm.addValueMeta(vi);
        assertEquals((byte) 1, connector.typeConvertion(rm.getValueMeta(2), StarRocksDataType.TINYINT, 1L));
        assertEquals((short) 300, connector.typeConvertion(rm.getValueMeta(2), StarRocksDataType.SMALLINT, 300L));
        assertEquals(50000, connector.typeConvertion(rm.getValueMeta(2), StarRocksDataType.INT, 50000L));

        // Test for Number
        ValueMetaNumber vn = new ValueMetaNumber("number");
        rm.addValueMeta(vn);
        assertEquals(100140.123, connector.typeConvertion(rm.getValueMeta(3), null, 100140.123));

        // Test for BigDecimal
        ValueMetaBigNumber vbg = new ValueMetaBigNumber("bignumber");
        rm.addValueMeta(vbg);
        BigDecimal bigDecimal = new BigDecimal("1000000.123");
        assertEquals(bigDecimal, connector.typeConvertion(rm.getValueMeta(4), null, bigDecimal));

        // Test for Date
        ValueMetaDate vd = new ValueMetaDate("date");
        rm.addValueMeta(vd);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormatter.parse("2022-08-05");
        assertEquals("2022-08-05", connector.typeConvertion(rm.getValueMeta(5), StarRocksDataType.DATE, date));

        // Test for Timestamp
        ValueMetaTimestamp vt = new ValueMetaTimestamp("timestamp");
        rm.addValueMeta(vt);
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime = datetimeFormat.parse("2022-08-05 12:34:56");
        Timestamp timestamp = new Timestamp(dateTime.getTime());
        assertEquals("2022-08-05 12:34:56", connector.typeConvertion(rm.getValueMeta(6), StarRocksDataType.DATETIME, timestamp));

        // Test for InetAddress
        ValueMetaInternetAddress vint = new ValueMetaInternetAddress("inetaddress");
        rm.addValueMeta(vint);
        assertEquals("93.184.216.34", connector.typeConvertion(rm.getValueMeta(7), null, "93.184.216.34"));
    }
}
