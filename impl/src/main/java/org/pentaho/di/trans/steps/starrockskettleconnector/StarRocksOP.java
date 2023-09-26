package org.pentaho.di.trans.steps.starrockskettleconnector;

public enum StarRocksOP {
    UPSERT, DELETE;
    public static final String COLUMN_KEY="__op";

    static StarRocksOP parse(String option){
        if (option.equals("UPSERT")){
            return UPSERT;
        }
        if (option.equals("DELETE")){
            return DELETE;
        }
        throw new RuntimeException("Unsupported row option.");
    }
}
