package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import org.pentaho.di.core.exception.KettleException;

import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

public class StarRocksJdbcConnectionOptions{

    protected final String url;
    protected final String driverName;
    protected final String cjDriverName;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;

    public StarRocksJdbcConnectionOptions(String url, String username, String password){
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
