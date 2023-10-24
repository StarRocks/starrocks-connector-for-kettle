package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import java.sql.Connection;

public interface StarRocksJdbcConnectionIProvider {
    Connection getConnection() throws Exception;

    void close();
}
