package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksISerializer;

import java.util.Map;

/**
 * Stores data for the StarRocks Kettle Connector step.
 */
public class StarRocksKettleConnectorData extends BaseStepData implements StepDataInterface {

    // Use the Stream Load method to load the number.
    public StreamLoadManagerV2 streamLoadManager;

    public StarRocksISerializer serializer;

    // In StarRocks,If you want to implement changes to the data and partial imports, you need to add '__op'.
    public String[] columns;

    //The index corresponding to the data type of the row element.
    public int[] keynrs; // nr of keylookup -value in row...

    // The field name and field type of the target table in Starrocks.
    public Map<String, StarRocksDataType> fieldtype;
    public String tablename;
    public String databasename;


    public StarRocksKettleConnectorData() {
        super();

        streamLoadManager = null;
    }
}
