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

package com.starrocks.connector.kettle.steps.starrockskettleconnector;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.*;

public class StarRocksKettleConnectorMetaTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    public class HttpUrlFieldLoadSaveValidator implements FieldLoadSaveValidator<List<String>> {
        private final int arraySize;

        public HttpUrlFieldLoadSaveValidator(int arraySize) {
            this.arraySize = arraySize;
        }

        @Override
        public List<String> getTestObject() {
            List<String> loadUrlList = new ArrayList<>();
            for (int i = 0; i < arraySize; i++) {
                loadUrlList.add("192.168.110.120:" + i);
            }
            return loadUrlList;
        }

        @Override
        public boolean validateTestObject(List<String> original, Object actual) {
            if (original == null || actual == null) {
                return original == actual;
            }
            return original.equals(actual);
        }
    }

    public class FloatFieldLoadSaveValidator implements FieldLoadSaveValidator<Float> {

        @Override
        public Float getTestObject() {
            return 123.45f;
        }

        @Override
        public boolean validateTestObject(Float aFloat, Object o) {
            if (o instanceof Float) {
                return aFloat.equals(o);
            }
            return false;
        }
    }

    @Test
    public void testRoundTrip() throws KettleException {
        List<String> attributes = Arrays.asList("httpurl", "jdbcurl", "databasename", "tablename", "user", "password", "format","columnseparator","jsonpaths","maxbytes","scanningFrequency",
                "max_filter_ratio", "connecttimeout", "timeout", "stream_name", "field_name", "partialupdate", "partialcolumns", "enableupsertdelete",
                "upsertordelete","headerProperties");

        Map<String, String> getterMap = new HashMap<>();
        getterMap.put("httpurl", "getHttpurl");
        getterMap.put("jdbcurl", "getJdbcurl");
        getterMap.put("databasename", "getDatabasename");
        getterMap.put("tablename", "getTablename");
        getterMap.put("user", "getUser");
        getterMap.put("password", "getPassword");
        getterMap.put("format", "getFormat");
        getterMap.put("jsonpaths","getJsonpaths");
        getterMap.put("columnseparator","getColumnSeparator");
        getterMap.put("maxbytes", "getMaxbytes");
        getterMap.put("scanningFrequency","getScanningFrequency");
        getterMap.put("max_filter_ratio", "getMaxFilterRatio");
        getterMap.put("connecttimeout", "getConnecttimeout");
        getterMap.put("timeout", "getTimeout");
        getterMap.put("stream_name", "getFieldTable");
        getterMap.put("field_name", "getFieldStream");
        getterMap.put("partialupdate", "getPartialUpdate");
        getterMap.put("partialcolumns", "getPartialcolumns");
        getterMap.put("enableupsertdelete", "getEnableUpsertDelete");
        getterMap.put("upsertordelete", "getUpsertOrDelete");
        getterMap.put("headerProperties","getHeaderProperties");

        Map<String, String> setterMap = new HashMap<>();
        setterMap.put("httpurl", "setHttpurl");
        setterMap.put("jdbcurl", "setJdbcurl");
        setterMap.put("databasename", "setDatabasename");
        setterMap.put("tablename", "setTablename");
        setterMap.put("user", "setUser");
        setterMap.put("password", "setPassword");
        setterMap.put("format", "setFormat");
        setterMap.put("columnseparator","setColumnSeparator");
        setterMap.put("jsonpaths","setJsonpaths");
        setterMap.put("maxbytes", "setMaxbytes");
        setterMap.put("scanningFrequency","setScanningFrequency");
        setterMap.put("max_filter_ratio", "setMaxFilterRatio");
        setterMap.put("connecttimeout", "setConnecttimeout");
        setterMap.put("timeout", "setTimeout");
        setterMap.put("stream_name", "setFieldTable");
        setterMap.put("field_name", "setFieldStream");
        setterMap.put("partialupdate", "setPartialupdate");
        setterMap.put("partialcolumns", "setPartialcolumns");
        setterMap.put("enableupsertdelete", "setEnableupsertdelete");
        setterMap.put("upsertordelete", "setUpsertOrDelete");
        setterMap.put("headerProperties","setHeaderProperties");

        Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
                new HashMap<String, FieldLoadSaveValidator<?>>();
//


        FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
                new ArrayLoadSaveValidator<String>(new StringLoadSaveValidator(), 25);


        FieldLoadSaveValidator<List<String>> httpUrlFieldLoadSaveValidator = new HttpUrlFieldLoadSaveValidator(5);
        FieldLoadSaveValidator<Float> floatFieldLoadSaveValidator = new FloatFieldLoadSaveValidator();
        fieldLoadSaveValidatorAttributeMap.put("httpurl", httpUrlFieldLoadSaveValidator);
        fieldLoadSaveValidatorAttributeMap.put("max_filter_ratio", floatFieldLoadSaveValidator);
        fieldLoadSaveValidatorAttributeMap.put("stream_name", stringArrayLoadSaveValidator);
        fieldLoadSaveValidatorAttributeMap.put("field_name", stringArrayLoadSaveValidator);

        LoadSaveTester loadSaveTester =
                new LoadSaveTester(StarRocksKettleConnectorMeta.class, attributes, getterMap, setterMap,
                        fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>());

        loadSaveTester.testSerialization();
    }

    @Test
    public void testPDIStarrocks() throws Exception {
        StarRocksKettleConnectorMeta starRocksKettleConnector= new StarRocksKettleConnectorMeta();
        starRocksKettleConnector.setFieldTable( new String[] { "table1", "table2", "table3" } );
        starRocksKettleConnector.setFieldStream( new String[] { "stream1" } );
        starRocksKettleConnector.setTablename( "test_tablename" );

        try {
            String badXml = starRocksKettleConnector.getXML();
            Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
        } catch ( Exception expected ) {
            // Do Nothing
        }
        starRocksKettleConnector.afterInjectionSynchronization();
        //run without a exception
        String ktrXml = starRocksKettleConnector.getXML();

        int targetSz = starRocksKettleConnector.getFieldTable().length;
        Assert.assertEquals( targetSz, starRocksKettleConnector.getFieldStream().length );

    }
}
