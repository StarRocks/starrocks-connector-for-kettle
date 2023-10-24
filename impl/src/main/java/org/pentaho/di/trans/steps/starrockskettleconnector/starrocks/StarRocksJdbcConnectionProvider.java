package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class StarRocksJdbcConnectionProvider implements StarRocksJdbcConnectionIProvider {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJdbcConnectionProvider.class);

    private final StarRocksJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public StarRocksJdbcConnectionProvider(StarRocksJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    try {
                        Class.forName(jdbcOptions.getCjDriverName());
                    } catch (Exception e) {
                        Class.forName(jdbcOptions.getDriverName());
                    }
                }
                if (jdbcOptions.getUsername().isPresent()) {
                    connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().orElse(null), jdbcOptions.getPassword().orElse(null));
                } else {
                    connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                }
            }
        }
        return connection;
    }


    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            LOG.error("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
    }
}
