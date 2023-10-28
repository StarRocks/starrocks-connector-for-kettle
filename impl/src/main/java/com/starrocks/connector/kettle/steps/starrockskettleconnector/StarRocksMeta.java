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
