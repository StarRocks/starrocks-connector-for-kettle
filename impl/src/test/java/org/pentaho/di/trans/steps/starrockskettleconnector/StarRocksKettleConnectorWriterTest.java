package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksKettleConnectorWriterTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    StarRocksKettleConnectorMeta lmeta;
    StarRocksKettleConnectorData ldata;
    StarRocksKettleConnector lder;

    RowMeta rm=new RowMeta();
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
        vars.put("httpurl", "127.0.0.1:8030");
        vars.put("jdbcurl", "jdbc:mysql://127.0.0.1:9030");
        vars.put("databasename", "kettle_test");
        vars.put("tablename", "student");
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
        lmeta.setMaxbytes(6);
        lmeta.setScanningFrequency(50L);
        lmeta.setConnecttimeout(1000);
        lmeta.setTimeout(600);
        lmeta.setMaxFilterRatio(0);
        lmeta.setColumnSeparator("\t");
        lmeta.setPartialupdate(true);
        lmeta.setPartialcolumns(new String[]{"id","name"});
        // lmeta.setJsonpaths("[\"$.id\", \"$.name\",\"$.score\"]");

//        lmeta.setFieldStream(new String[]{"id","name","score"});
//        lmeta.setFieldTable(new String[]{"id","name","score"});
//        ValueMetaInteger vi=new ValueMetaInteger("id");
//        ValueMetaString vs=new ValueMetaString("name");
//        ValueMetaInteger vn=new ValueMetaInteger("score");
//        rm.addValueMeta(vi);
//        rm.addValueMeta(vs);
//        rm.addValueMeta(vn);
        lmeta.setFieldStream(new String[]{"id","name"});
        lmeta.setFieldTable(new String[]{"id","name"});
        ValueMetaInteger vi=new ValueMetaInteger("id");
        ValueMetaString vs=new ValueMetaString("name");
        rm.addValueMeta(vi);
        rm.addValueMeta(vs);

        ldata = new StarRocksKettleConnectorData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String skcPid = plugReg.getPluginId(StepPluginType.class, lmeta);
        smeta = new StepMeta(skcPid, "StarRocksKettleConnector", lmeta);
        Trans trans = new Trans(transMeta);
        transMeta.addStep(smeta);
        lder = new StarRocksKettleConnector(smeta, ldata, 1, transMeta, trans);
        lder.setInputRowMeta(rm);
        lder.copyVariablesFrom(transMeta);

    }
    @Test
    public void testStreamLoad() throws KettleException{
        // lder.init(lmeta,ldata);
        // lder.processRow(lmeta,ldata);
    }
}
