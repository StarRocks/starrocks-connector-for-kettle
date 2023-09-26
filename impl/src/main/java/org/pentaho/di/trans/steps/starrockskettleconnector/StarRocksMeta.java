package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

public interface StarRocksMeta extends StepMetaInterface {
    List<String> getHttpurl();

    String getJdbcurl();

    String getDatabasename();

    String getTablename();

    String getUser();

    String getPassword();

    String getFormat();

    String getColumnSeparator();

    String getJsonpaths();

    long getMaxbytes();

    float getMaxFilterRatio();

    int getConnecttimeout();

    int getTimeout();
    boolean getPartialUpdate();
    String[] getPartialcolumns();

    boolean getEnableUpsertDelete();
    String getUpsertOrDelete();

    long getScanningFrequency();
}
