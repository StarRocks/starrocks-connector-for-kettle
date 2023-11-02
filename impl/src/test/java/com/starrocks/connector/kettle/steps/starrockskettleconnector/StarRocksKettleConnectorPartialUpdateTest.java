package com.starrocks.connector.kettle.steps.starrockskettleconnector;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

public class StarRocksKettleConnectorPartialUpdateTest extends StarRocksConnectorBaseTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    StarRocksKettleConnectorMeta lmeta;
    StarRocksKettleConnectorData ldata;
    StarRocksKettleConnector lder;

    RowMeta rm = new RowMeta();
    StepMeta smeta;

    @BeforeClass
    public static void initEnvironment() throws Exception {
        KettleEnvironment.init();
    }


    /**
     * Test partial update
     *
     * @throws Exception
     */
    @Test
    public void testKettlePartialUpdateStarRocks() throws Exception {


        TransMeta transMeta = new TransMeta();
        transMeta.setName("StarRocksKettleConnector");

        Map<String, String> vars = new HashMap<>();
        vars.put("httpurl", getHttpUrls());
        vars.put("jdbcurl", getJdbcUrl());
        vars.put("databasename", getDbName());
        vars.put("tablename", getTableName());
        vars.put("user", getUSER());
        vars.put("password", getPASSWORD());
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
        lmeta.setMaxbytes(6);
        lmeta.setScanningFrequency(50L);
        lmeta.setConnecttimeout(1000);
        lmeta.setTimeout(600);
        lmeta.setMaxFilterRatio(0);
        lmeta.setColumnSeparator("\t");
        lmeta.setFieldStream(new String[]{"id", "name"});
        lmeta.setFieldTable(new String[]{"id", "name"});
        lmeta.setPartialupdate(true);
        lmeta.setPartialcolumns(new String[]{"id", "name"});


        ldata = new StarRocksKettleConnectorData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String skcPid = plugReg.getPluginId(StepPluginType.class, lmeta);
        smeta = new StepMeta(skcPid, "StarRocksKettleConnector", lmeta);
        Trans trans = new Trans(transMeta);
        transMeta.addStep(smeta);
        lder = Mockito.spy(new StarRocksKettleConnector(smeta, ldata, 1, transMeta, trans));
        ValueMetaInteger vi = new ValueMetaInteger("id");
        ValueMetaString vs = new ValueMetaString("name");
        rm.addValueMeta(vi);
        rm.addValueMeta(vs);


        lder.setInputRowMeta(rm);
        lder.copyVariablesFrom(transMeta);
        StarRocksKettleConnectorWriteTest.createStarRocksTable();
        lder.init(lmeta, ldata);

        // test partial update
        long a = 1;
        String b = "test";
        Object[] r = new Object[]{(Object) a, (Object) b};
        doReturn(r).when(lder).getRow();
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                lder.dispose(lmeta, ldata);
                return null;
            }
        }).when(lder).putRow(any(), any());
        lder.processRow(lmeta, ldata);
        String dbSource = selectTheTable();
        assertEquals("1testnull", dbSource);
    }
}
