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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.pentaho.di.core.KettleEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

import static org.junit.Assume.assumeTrue;

public abstract class StarRocksConnectorBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksConnectorBaseTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    protected static String DB_NAME;
    protected static String HTTP_URLS;
    protected static String JDBC_URLS;
    protected static String TABLE_NAME;
    protected static String USER;
    protected static String PASSWORD;

    @BeforeClass
    public static void initEnvironment() throws Exception {
        KettleEnvironment.init();
    }

    protected static String getHttpUrls() {
        return HTTP_URLS;
    }

    protected static String getJdbcUrl() {
        return JDBC_URLS;
    }

    protected static String getTableName() {
        return TABLE_NAME;
    }

    protected static String getDbName() {
        return DB_NAME;
    }

    protected static String getUSER() {
        return USER;
    }

    protected static String getPASSWORD() {
        return PASSWORD;
    }

    protected static Connection DB_CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        HTTP_URLS = System.getProperty("http_urls");
        JDBC_URLS = System.getProperty("jdbc_urls");
        assumeTrue(HTTP_URLS != null && JDBC_URLS != null);
        USER = System.getProperty("user");
        PASSWORD = System.getProperty("password");
        USER = (USER == null ? "root" : USER);
        PASSWORD = (PASSWORD == null ? "" : PASSWORD);
        LOG.info("StarRocks login information:user=" + USER + " password=" + (PASSWORD == null ? "" : PASSWORD));


        DB_NAME = "sr_write_test_" + genRandomUuid();
        TABLE_NAME = "sr_write_test_table_" + genRandomUuid();
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD);
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }

        try {
            String createDb = "CREATE DATABASE " + DB_NAME;
            executeSrSQL(createDb);
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME);
                executeSrSQL(dropDb);
                LOG.info("Successful to drop database {}", DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", DB_NAME, e);
            }
            DB_CONNECTION.close();
        }
    }

    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }

    protected static String selectTheTable() throws Exception {
        String[] ret = new String[3];
        if (DB_CONNECTION != null) {
            String selectTable = String.format("SELECT * FROM %s.%s", DB_NAME, TABLE_NAME);
            try (PreparedStatement statement = DB_CONNECTION.prepareStatement(selectTable)) {
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    ret[0] = resultSet.getString("id");
                    ret[1] = resultSet.getString("name");
                    ret[2] = resultSet.getString("score");
                }
            }
        }
        LOG.info(String.format("Get the database data as [%s]", String.join(",", ret)));
        return String.join("", ret);
    }
}
