package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.alibaba.fastjson.JSON;
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
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarRocksKettleConnectorTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

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
        TransMeta transMeta = new TransMeta();
        transMeta.setName("StarRocksKettleConnector");

        Map<String, String> vars = new HashMap<>();
        vars.put("httpurl", "10.112.133.149:8030;10.112.143.215:8030;10.112.156.187:8030");
        vars.put("jdbcurl", "jdbc:mysql://10.112.133.149:9030");
        vars.put("databasename", "somedatabase");
        vars.put("tablename", "sometable");
        vars.put("user", "root");
        vars.put("password", "");
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
        assertEquals(lmeta.getColumnSeparator(),newMeta.getColumnSeparator());
        assertEquals(lmeta.getJsonpaths(),newMeta.getJsonpaths());
    }

    @Test
    public void testVariableSubstitution() throws KettleException {
        lmeta.setFieldTable(new String[0]);
        lder.init(lmeta, ldata);

        assertEquals("somedatabase", ldata.databasename);
        assertEquals("sometable", ldata.tablename);

    }

    @Test
    public void testTypeConvertionForAllTypes() throws KettleException, Exception {

        StarRocksKettleConnector connector = lder;
        RowMeta rm = new RowMeta();
        // Test for String
        ValueMetaString vs = new ValueMetaString("string");
        rm.addValueMeta(vs);
        assertEquals("normalString", connector.typeConvertion(rm.getValueMeta(0), null, "normalString"));
        assertEquals(JSON.parse("{\"test\":\"data\"}"), connector.typeConvertion(rm.getValueMeta(0), StarRocksDataType.JSON, "{\"test\":\"data\"}"));

        // Test for Boolean
        ValueMetaBoolean vb=new ValueMetaBoolean("boolean");
        rm.addValueMeta(vb);
        assertEquals(true, connector.typeConvertion(rm.getValueMeta(1), null, true));
        assertEquals(false, connector.typeConvertion(rm.getValueMeta(1), null, false));

        // Test for Integer
        ValueMetaInteger vi=new ValueMetaInteger("integer");
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
        ValueMetaDate vd=new ValueMetaDate("date");
        rm.addValueMeta(vd);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormatter.parse("2022-08-05");
        assertEquals("2022-08-05", connector.typeConvertion(rm.getValueMeta(5), StarRocksDataType.DATE, date));

        // Test for Timestamp
        ValueMetaTimestamp vt=new ValueMetaTimestamp("timestamp");
        rm.addValueMeta(vt);
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime = datetimeFormat.parse("2022-08-05 12:34:56");
        Timestamp timestamp=new Timestamp(dateTime.getTime());
        assertEquals("2022-08-05 12:34:56", connector.typeConvertion(rm.getValueMeta(6), StarRocksDataType.DATETIME, timestamp));

        // Test for InetAddress
        ValueMetaInternetAddress vint=new ValueMetaInternetAddress("inetaddress");
        rm.addValueMeta(vint);
        assertEquals("93.184.216.34", connector.typeConvertion(rm.getValueMeta(7), null, "93.184.216.34"));
    }

}
