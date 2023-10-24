package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import javax.annotation.Nullable;
import java.util.Optional;

public class StarRocksJdbcConnectionOptions {

    private final String url;
    private final String driverName;
    private final String cjDriverName;
    @Nullable
    private final String username;
    @Nullable
    private final String password;

    public StarRocksJdbcConnectionOptions(String url, String username, String password) {
        this.url = url;
        this.driverName = "com.mysql.jdbc.Driver";
        this.cjDriverName = "com.mysql.cj.jdbc.Driver";
        this.username = username;
        this.password = password;
    }

    public String getDbURL() {
        return url;
    }

    public String getCjDriverName() {
        return cjDriverName;
    }

    public String getDriverName() {
        return driverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
