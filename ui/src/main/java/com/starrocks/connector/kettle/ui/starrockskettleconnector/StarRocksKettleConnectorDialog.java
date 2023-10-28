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

package com.starrocks.connector.kettle.ui.starrockskettleconnector;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.StarRocksKettleConnectorMeta;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.kettle.steps.starrockskettleconnector.starrocks.StarRocksQueryVisitor;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.List;
import java.util.*;

/**
 * Dialog class for the StarRocks Kettle Connector step.
 */
@PluginDialog(id = "StarRocksKettleConnector", image = "StarRocks.svg", pluginType = PluginDialog.PluginType.STEP,
        documentationUrl = "https://docs.starrocks.io/zh-cn/latest/introduction/StarRocks_intro")
public class StarRocksKettleConnectorDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = StarRocksKettleConnectorDialog.class;

    private Label wlHttpurl;
    private TextVar wHttpurl;
    private FormData fdlHttpurl, fdHttpurl;

    private Label wlJdbcUrl;
    private TextVar wJdbcUrl;
    private FormData fdlJdbcUrl, fdJdbcUrl;

    private Label wlDatabaseName;
    private TextVar wDatabaseName;
    private FormData fdlDatabaseName, fdDatabaseName;

    private Label wlTableName;
    private TextVar wTableName;
    private FormData fdlTableName, fdTableName;

    private Label wlUser;
    private TextVar wUser;
    private FormData fdlUser, fdUser;

    private Label wlPassword;
    private TextVar wPassword;
    private FormData fdlPassword, fdPassword;

    private Label wlFormat;
    private CCombo wFormat;
    private FormData fdlFormat, fdFormat;

    private Label wlColumnSeparator;
    private Button wbColumnSeparator;
    private TextVar wColumnSeparator;
    private FormData fdlColumnSeparator, fdColumnSeparator;

    private Label wlJsonPaths;
    private TextVar wJsonPaths;
    private FormData fdlJsonPaths, fdJsonPaths;

    private Label wlMaxBytes;
    private TextVar wMaxBytes;
    private FormData fdlMaxBytes, fdMaxBytes;

    private Label wlMaxFilterRatio;
    private TextVar wMaxFilterRatio;
    private FormData fdlMaxFilterRatio, fdMaxFilterRatio;

    private Label wlConnectTimeout;
    private TextVar wConnectTimeout;
    private FormData fdlConnectTimeout, fdConnectTimeout;

    private Label wlTimeout;
    private TextVar wTimeout;
    private FormData fdlTimeout, fdTimeout;

    private Label wlPartialUpdate;
    private Button wPartialUpdate;
    private FormData fdlPartialUpdate, fdPartialUpdate;

    private Label wlPartialColumns;
    private TextVar wPartialColumns;
    private FormData fdlPartialColumns, fdPartialColumns;

    private Label wlEnableUpsertDelete;
    private Button wEnableUpsertDelete;
    private FormData fdlEnableUpsertDelete, fdEnableUpsertDelete;

    private Label wlUpsertorDelete;
    private CCombo wUpsertorDelete;
    private FormData fdlUpsertorDelete, fdUpsertorDelete;

    private Label wlScanningFrequency;
    private TextVar wScanningFrequency;
    private FormData fdlScanningFrequency, fdScanningFrequency;

    private Label wlHeaderProperties;
    private TextVar wHeaderProperties;
    private FormData fdlHeaderProperties,fdHeaderProperties;

    private Label wlReturn;
    private TableView wReturn;
    private FormData fdlReturn, fdReturn;

    private Button wGetLU;
    private FormData fdGetLU;
    private Listener lsGetLU;

    private Button wDoMapping;
    private FormData fdDoMapping;

    private StarRocksKettleConnectorMeta input;
    private ColumnInfo[] ciReturn;
    private Map<String, Integer> inputFields;
    private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

    public StarRocksKettleConnectorDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (StarRocksKettleConnectorMeta) in;
        inputFields = new HashMap<String, Integer>();
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);

        ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                input.setChanged();
            }
        };

        FocusListener lsFocusLost = new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent focusEvent) {
                setTableFieldCombo();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname = new FormData();
        fdlStepname.left = new FormAttachment(0, 0);
        fdlStepname.right = new FormAttachment(middle, -margin);
        fdlStepname.top = new FormAttachment(0, margin);
        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(lsMod);
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);
        wStepname.setLayoutData(fdStepname);

        // Http Url line...
        wlHttpurl = new Label(shell, SWT.RIGHT);
        wlHttpurl.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.HttpUrl.Label"));
        props.setLook(wlHttpurl);
        fdlHttpurl = new FormData();
        fdlHttpurl.left = new FormAttachment(0, 0);
        fdlHttpurl.right = new FormAttachment(middle, -margin);
        fdlHttpurl.top = new FormAttachment(wStepname, margin * 2);
        wlHttpurl.setLayoutData(fdlHttpurl);

        wHttpurl = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wHttpurl);
        wHttpurl.addModifyListener(lsMod);
        wHttpurl.addFocusListener(lsFocusLost);
        fdHttpurl = new FormData();
        fdHttpurl.left = new FormAttachment(middle, 0);
        fdHttpurl.right = new FormAttachment(100, 0);
        fdHttpurl.top = new FormAttachment(wStepname, margin * 2);
        wHttpurl.setLayoutData(fdHttpurl);

        // JDBC Url ...
        wlJdbcUrl = new Label(shell, SWT.RIGHT);
        wlJdbcUrl.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.JdbcUrl.Label"));
        props.setLook(wlJdbcUrl);
        fdlJdbcUrl = new FormData();
        fdlJdbcUrl.left = new FormAttachment(0, 0);
        fdlJdbcUrl.right = new FormAttachment(middle, -margin);
        fdlJdbcUrl.top = new FormAttachment(wHttpurl, margin * 2);
        wlJdbcUrl.setLayoutData(fdlJdbcUrl);

        wJdbcUrl = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wJdbcUrl);
        wJdbcUrl.addModifyListener(lsMod);
        wJdbcUrl.addFocusListener(lsFocusLost);
        fdJdbcUrl = new FormData();
        fdJdbcUrl.left = new FormAttachment(middle, 0);
        fdJdbcUrl.right = new FormAttachment(100, 0);
        fdJdbcUrl.top = new FormAttachment(wHttpurl, margin * 2);
        wJdbcUrl.setLayoutData(fdJdbcUrl);

        // DataBase Name line...
        wlDatabaseName = new Label(shell, SWT.RIGHT);
        wlDatabaseName.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DatabaseName.Label"));
        props.setLook(wlDatabaseName);
        fdlDatabaseName = new FormData();
        fdlDatabaseName.left = new FormAttachment(0, 0);
        fdlDatabaseName.right = new FormAttachment(middle, -margin);
        fdlDatabaseName.top = new FormAttachment(wJdbcUrl, margin * 2);
        wlDatabaseName.setLayoutData(fdlDatabaseName);

        wDatabaseName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wDatabaseName);
        wDatabaseName.addModifyListener(lsMod);
        wDatabaseName.addFocusListener(lsFocusLost);
        fdDatabaseName = new FormData();
        fdDatabaseName.left = new FormAttachment(middle, 0);
        fdDatabaseName.right = new FormAttachment(100, 0);
        fdDatabaseName.top = new FormAttachment(wJdbcUrl, margin * 2);
        wDatabaseName.setLayoutData(fdDatabaseName);

        // Table Name line...
        wlTableName = new Label(shell, SWT.RIGHT);
        wlTableName.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.TableName.Label"));
        props.setLook(wlTableName);
        fdlTableName = new FormData();
        fdlTableName.left = new FormAttachment(0, 0);
        fdlTableName.right = new FormAttachment(middle, -margin);
        fdlTableName.top = new FormAttachment(wDatabaseName, margin * 2);
        wlTableName.setLayoutData(fdlTableName);

        wTableName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wTableName);
        wTableName.addModifyListener(lsMod);
        wTableName.addFocusListener(lsFocusLost);
        wTableName.setText(input.getTablename());
        fdTableName = new FormData();
        fdTableName.left = new FormAttachment(middle, 0);
        fdTableName.right = new FormAttachment(100, 0);
        fdTableName.top = new FormAttachment(wDatabaseName, margin * 2);
        wTableName.setLayoutData(fdTableName);

        // User line...
        wlUser = new Label(shell, SWT.RIGHT);
        wlUser.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.User.Label"));
        props.setLook(wlUser);
        fdlUser = new FormData();
        fdlUser.left = new FormAttachment(0, 0);
        fdlUser.right = new FormAttachment(middle, -margin);
        fdlUser.top = new FormAttachment(wTableName, margin * 2);
        wlUser.setLayoutData(fdlUser);

        wUser = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wUser);
        wUser.addModifyListener(lsMod);
        wUser.addFocusListener(lsFocusLost);
        fdUser = new FormData();
        fdUser.left = new FormAttachment(middle, 0);
        fdUser.right = new FormAttachment(100, 0);
        fdUser.top = new FormAttachment(wTableName, margin * 2);
        wUser.setLayoutData(fdUser);

        // Password line ...
        wlPassword = new Label(shell, SWT.RIGHT);
        wlPassword.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Password.Label"));
        props.setLook(wlPassword);
        fdlPassword = new FormData();
        fdlPassword.left = new FormAttachment(0, 0);
        fdlPassword.right = new FormAttachment(middle, -margin);
        fdlPassword.top = new FormAttachment(wUser, margin * 2);
        wlPassword.setLayoutData(fdlPassword);

        wPassword = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wPassword);
        wPassword.addModifyListener(lsMod);
        wPassword.addFocusListener(lsFocusLost);
        fdPassword = new FormData();
        fdPassword.left = new FormAttachment(middle, 0);
        fdPassword.right = new FormAttachment(100, 0);
        fdPassword.top = new FormAttachment(wUser, margin * 2);
        wPassword.setLayoutData(fdPassword);

        // Format line...
        wlFormat = new Label(shell, SWT.RIGHT);
        wlFormat.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Format.Label"));
        props.setLook(wlFormat);
        fdlFormat = new FormData();
        fdlFormat.left = new FormAttachment(0, 0);
        fdlFormat.right = new FormAttachment(middle, -margin);
        fdlFormat.top = new FormAttachment(wPassword, margin * 2);
        wlFormat.setLayoutData(fdlFormat);

        wFormat = new CCombo(shell, SWT.BORDER);
        props.setLook(wFormat);
        wFormat.addModifyListener(lsMod);
        wFormat.setItems(new String[]{"CSV", "JSON"});
        wFormat.select(0); // select CSV by default
        fdFormat = new FormData();
        fdFormat.left = new FormAttachment(middle, 0);
        fdFormat.top = new FormAttachment(wPassword, margin * 2);
        fdFormat.right = new FormAttachment(100, 0);
        wFormat.setLayoutData(fdFormat);

        // Column Separator line...
        wlColumnSeparator = new Label(shell, SWT.RIGHT);
        wlColumnSeparator.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ColumnSeparator.Label"));
        props.setLook(wlColumnSeparator);
        fdlColumnSeparator = new FormData();
        fdlColumnSeparator.left = new FormAttachment(0, 0);
        fdlColumnSeparator.right = new FormAttachment(middle, -margin);
        fdlColumnSeparator.top = new FormAttachment(wFormat, margin);
        wlColumnSeparator.setLayoutData(fdlColumnSeparator);

        wbColumnSeparator = new Button(shell, SWT.PUSH | SWT.CENTER);
        props.setLook(wbColumnSeparator);
        wbColumnSeparator.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ColumnSeparator.Button"));
        FormData fdbColumnSeparator = new FormData();
        fdbColumnSeparator.top = new FormAttachment(wFormat, margin);
        fdbColumnSeparator.right = new FormAttachment(100, 0);
        wbColumnSeparator.setLayoutData(fdbColumnSeparator);

        wColumnSeparator = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wColumnSeparator);
        wColumnSeparator.addModifyListener(lsMod);
        fdColumnSeparator = new FormData();
        fdColumnSeparator.left = new FormAttachment(middle, 0);
        fdColumnSeparator.top = new FormAttachment(wFormat, margin);
        fdColumnSeparator.right = new FormAttachment(wbColumnSeparator, -margin);
        wColumnSeparator.setLayoutData(fdColumnSeparator);
        // Allow the insertion of tabs as separator...
        wbColumnSeparator.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent se) {
                Text t = wColumnSeparator.getTextWidget();
                if (t != null) {
                    t.insert("\t");
                }
            }
        });

        // Json Paths line..
        wlJsonPaths = new Label(shell, SWT.RIGHT);
        wlJsonPaths.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.JsonPaths.Label"));
        props.setLook(wlJsonPaths);
        fdlJsonPaths = new FormData();
        fdlJsonPaths.left = new FormAttachment(0, 0);
        fdlJsonPaths.right = new FormAttachment(middle, -margin);
        fdlJsonPaths.top = new FormAttachment(wColumnSeparator, margin * 2);
        wlJsonPaths.setLayoutData(fdlJsonPaths);

        wJsonPaths = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wJsonPaths);
        wJsonPaths.addModifyListener(lsMod);
        fdJsonPaths = new FormData();
        fdJsonPaths.left = new FormAttachment(middle, 0);
        fdJsonPaths.top = new FormAttachment(wColumnSeparator, margin * 2);
        fdJsonPaths.right = new FormAttachment(100, 0);
        wJsonPaths.setLayoutData(fdJsonPaths);

        // Max Bytes line...
        wlMaxBytes = new Label(shell, SWT.RIGHT);
        wlMaxBytes.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.MaxBytes.Label"));
        props.setLook(wlMaxBytes);
        fdlMaxBytes = new FormData();
        fdlMaxBytes.left = new FormAttachment(0, 0);
        fdlMaxBytes.right = new FormAttachment(middle, -margin);
        fdlMaxBytes.top = new FormAttachment(wJsonPaths, margin * 2);
        wlMaxBytes.setLayoutData(fdlMaxBytes);

        wMaxBytes = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMaxBytes);
        wMaxBytes.addModifyListener(lsMod);
        fdMaxBytes = new FormData();
        fdMaxBytes.left = new FormAttachment(middle, 0);
        fdMaxBytes.top = new FormAttachment(wJsonPaths, margin * 2);
        fdMaxBytes.right = new FormAttachment(100, 0);
        wMaxBytes.setLayoutData(fdMaxBytes);

        //Scanning Frequency line...
        wlScanningFrequency = new Label(shell, SWT.RIGHT);
        wlScanningFrequency.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ScanningFrequency.Label"));
        props.setLook(wlScanningFrequency);
        fdlScanningFrequency = new FormData();
        fdlScanningFrequency.left = new FormAttachment(0, 0);
        fdlScanningFrequency.right = new FormAttachment(middle, -margin);
        fdlScanningFrequency.top = new FormAttachment(wMaxBytes, margin * 2);
        wlScanningFrequency.setLayoutData(fdlScanningFrequency);

        wScanningFrequency = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wScanningFrequency);
        wScanningFrequency.addModifyListener(lsMod);
        fdScanningFrequency = new FormData();
        fdScanningFrequency.left = new FormAttachment(middle, 0);
        fdScanningFrequency.top = new FormAttachment(wMaxBytes, margin * 2);
        fdScanningFrequency.right = new FormAttachment(100, 0);
        wScanningFrequency.setLayoutData(fdScanningFrequency);

        // Max Filter Ratio line...
        wlMaxFilterRatio = new Label(shell, SWT.RIGHT);
        wlMaxFilterRatio.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.MaxFilterRatio.Label"));
        props.setLook(wlMaxFilterRatio);
        fdlMaxFilterRatio = new FormData();
        fdlMaxFilterRatio.left = new FormAttachment(0, 0);
        fdlMaxFilterRatio.right = new FormAttachment(middle, -margin);
        fdlMaxFilterRatio.top = new FormAttachment(wScanningFrequency, margin * 2);
        wlMaxFilterRatio.setLayoutData(fdlMaxFilterRatio);

        wMaxFilterRatio = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wMaxFilterRatio);
        wMaxFilterRatio.addModifyListener(lsMod);
        fdMaxFilterRatio = new FormData();
        fdMaxFilterRatio.left = new FormAttachment(middle, 0);
        fdMaxFilterRatio.top = new FormAttachment(wScanningFrequency, margin * 2);
        fdMaxFilterRatio.right = new FormAttachment(100, 0);
        wMaxFilterRatio.setLayoutData(fdMaxFilterRatio);

        // Connect Timeout line ...
        wlConnectTimeout = new Label(shell, SWT.RIGHT);
        wlConnectTimeout.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ConnectTimeout.Label"));
        props.setLook(wlConnectTimeout);
        fdlConnectTimeout = new FormData();
        fdlConnectTimeout.left = new FormAttachment(0, 0);
        fdlConnectTimeout.right = new FormAttachment(middle, -margin);
        fdlConnectTimeout.top = new FormAttachment(wMaxFilterRatio, margin * 2);
        wlConnectTimeout.setLayoutData(fdlConnectTimeout);

        wConnectTimeout = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wConnectTimeout);
        wConnectTimeout.addModifyListener(lsMod);
        fdConnectTimeout = new FormData();
        fdConnectTimeout.left = new FormAttachment(middle, 0);
        fdConnectTimeout.top = new FormAttachment(wMaxFilterRatio, margin * 2);
        fdConnectTimeout.right = new FormAttachment(100, 0);
        wConnectTimeout.setLayoutData(fdConnectTimeout);

        // Stream Load Timeout line...
        wlTimeout = new Label(shell, SWT.RIGHT);
        wlTimeout.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Timeout.Label"));
        props.setLook(wlTimeout);
        fdlTimeout = new FormData();
        fdlTimeout.left = new FormAttachment(0, 0);
        fdlTimeout.right = new FormAttachment(middle, -margin);
        fdlTimeout.top = new FormAttachment(wConnectTimeout, margin * 2);
        wlTimeout.setLayoutData(fdlTimeout);

        wTimeout = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wTimeout);
        wTimeout.addModifyListener(lsMod);
        fdTimeout = new FormData();
        fdTimeout.left = new FormAttachment(middle, 0);
        fdTimeout.top = new FormAttachment(wConnectTimeout, margin * 2);
        fdTimeout.right = new FormAttachment(100, 0);
        wTimeout.setLayoutData(fdTimeout);

        // Stream Load Properties line...
        wlHeaderProperties = new Label(shell, SWT.RIGHT);
        wlHeaderProperties.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.HeaderProperties.Label"));
        props.setLook(wlHeaderProperties);
        fdlHeaderProperties = new FormData();
        fdlHeaderProperties.left = new FormAttachment(0, 0);
        fdlHeaderProperties.right = new FormAttachment(middle, -margin);
        fdlHeaderProperties.top = new FormAttachment(wTimeout, margin * 2);
        wlHeaderProperties.setLayoutData(fdlHeaderProperties);

        wHeaderProperties = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wHeaderProperties);
        wHeaderProperties.addModifyListener(lsMod);
        fdHeaderProperties = new FormData();
        fdHeaderProperties.left = new FormAttachment(middle, 0);
        fdHeaderProperties.top = new FormAttachment(wTimeout, margin * 2);
        fdHeaderProperties.right = new FormAttachment(100, 0);
        wHeaderProperties.setLayoutData(fdHeaderProperties);

        // Partial Update line...
        wlPartialUpdate = new Label(shell, SWT.RIGHT);
        wlPartialUpdate.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.PartialUpdate.Label"));
        props.setLook(wlPartialUpdate);
        fdlPartialUpdate = new FormData();
        fdlPartialUpdate.left = new FormAttachment(0, 0);
        fdlPartialUpdate.right = new FormAttachment(middle, -margin);
        fdlPartialUpdate.top = new FormAttachment(wHeaderProperties, margin * 2);
        wlPartialUpdate.setLayoutData(fdlPartialUpdate);

        wPartialUpdate = new Button(shell, SWT.CHECK | SWT.LEFT);
        props.setLook(wPartialUpdate);
        wPartialUpdate.setSelection(false);
        fdPartialUpdate = new FormData();
        fdPartialUpdate.left = new FormAttachment(middle, 0);
        fdPartialUpdate.right = new FormAttachment(100, 0);
        fdPartialUpdate.top = new FormAttachment(wHeaderProperties, margin * 2);
        wPartialUpdate.setLayoutData(fdPartialUpdate);
        wPartialUpdate.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent selectionEvent) {
                input.setChanged();
            }
        });

        // Partial Update Columns line ...
        wlPartialColumns = new Label(shell, SWT.RIGHT);
        wlPartialColumns.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.PartialColumns.Label"));
        props.setLook(wlPartialColumns);
        fdlPartialColumns = new FormData();
        fdlPartialColumns.left = new FormAttachment(0, 0);
        fdlPartialColumns.right = new FormAttachment(middle, -margin);
        fdlPartialColumns.top = new FormAttachment(wPartialUpdate, margin * 2);
        wlPartialColumns.setLayoutData(fdlPartialColumns);

        wPartialColumns = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wPartialColumns);
        wPartialColumns.addModifyListener(lsMod);
        fdPartialColumns = new FormData();
        fdPartialColumns.left = new FormAttachment(middle, 0);
        fdPartialColumns.top = new FormAttachment(wPartialUpdate, margin * 2);
        fdPartialColumns.right = new FormAttachment(100, 0);
        wPartialColumns.setLayoutData(fdPartialColumns);

        // Enable Upsert Delete
        wlEnableUpsertDelete = new Label(shell, SWT.RIGHT);
        wlEnableUpsertDelete.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.EnableUpsertDelete.Label"));
        props.setLook(wlEnableUpsertDelete);
        fdlEnableUpsertDelete = new FormData();
        fdlEnableUpsertDelete.left = new FormAttachment(0, 0);
        fdlEnableUpsertDelete.right = new FormAttachment(middle, -margin);
        fdlEnableUpsertDelete.top = new FormAttachment(wPartialColumns, margin * 2);
        wlEnableUpsertDelete.setLayoutData(fdlEnableUpsertDelete);

        wEnableUpsertDelete = new Button(shell, SWT.CHECK | SWT.LEFT);
        props.setLook(wEnableUpsertDelete);
        wEnableUpsertDelete.setSelection(false);
        fdEnableUpsertDelete = new FormData();
        fdEnableUpsertDelete.left = new FormAttachment(middle, 0);
        fdEnableUpsertDelete.right = new FormAttachment(100, 0);
        fdEnableUpsertDelete.top = new FormAttachment(wPartialColumns, margin * 2);
        wEnableUpsertDelete.setLayoutData(fdEnableUpsertDelete);
        wEnableUpsertDelete.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent selectionEvent) {
                input.setChanged();
            }
        });

        // Upsert or Delete line...
        wlUpsertorDelete = new Label(shell, SWT.RIGHT);
        wlUpsertorDelete.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.UpsertorDelete.Label"));
        props.setLook(wlUpsertorDelete);
        fdlUpsertorDelete = new FormData();
        fdlUpsertorDelete.left = new FormAttachment(0, 0);
        fdlUpsertorDelete.right = new FormAttachment(middle, -margin);
        fdlUpsertorDelete.top = new FormAttachment(wEnableUpsertDelete, margin * 2);
        wlUpsertorDelete.setLayoutData(fdlUpsertorDelete);

        wUpsertorDelete = new CCombo(shell, SWT.BORDER);
        props.setLook(wUpsertorDelete);
        wUpsertorDelete.addModifyListener(lsMod);
        wUpsertorDelete.setItems(new String[]{"", "UPSERT", "DELETE"});
        wUpsertorDelete.select(0);
        fdUpsertorDelete = new FormData();
        fdUpsertorDelete.left = new FormAttachment(middle, 0);
        fdUpsertorDelete.top = new FormAttachment(wEnableUpsertDelete, margin * 2);
        fdUpsertorDelete.right = new FormAttachment(100, 0);
        wUpsertorDelete.setLayoutData(fdUpsertorDelete);

        // The Buttons
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        setButtonPositions(new Button[]{wOK, wCancel}, margin, null);

        // The field Table
        wlReturn = new Label(shell, SWT.NONE);
        wlReturn.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Fields.Label"));
        props.setLook(wlReturn);
        fdlReturn = new FormData();
        fdlReturn.left = new FormAttachment(0, 0);
        fdlReturn.top = new FormAttachment(wUpsertorDelete, margin);
        wlReturn.setLayoutData(fdlReturn);

        int UpInsCols = 2;
        int UpInsRows = (input.getFieldTable() != null ? input.getFieldTable().length : 1);

        ciReturn = new ColumnInfo[UpInsCols];
        ciReturn[0] =
                new ColumnInfo(
                        BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ColumnInfo.TableField"),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false);
        ciReturn[1] =
                new ColumnInfo(
                        BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.ColumnInfo.StreamField"),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{""}, false);

        tableFieldColumns.add(ciReturn[0]);
        wReturn =
                new TableView(
                        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
                        UpInsRows, lsMod, props);

        wGetLU = new Button(shell, SWT.PUSH);
        wGetLU.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.GetFields.Label"));
        fdGetLU = new FormData();
        fdGetLU.top = new FormAttachment(wlReturn, margin);
        fdGetLU.right = new FormAttachment(100, 0);
        wGetLU.setLayoutData(fdGetLU);

        wDoMapping = new Button(shell, SWT.PUSH);
        wDoMapping.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.EditMapping.Label"));
        fdDoMapping = new FormData();
        fdDoMapping.top = new FormAttachment(wGetLU, margin);
        fdDoMapping.right = new FormAttachment(100, 0);
        wDoMapping.setLayoutData(fdDoMapping);

        wDoMapping.addListener(SWT.Selection, new Listener() {
            public void handleEvent(Event arg0) {
                generateMappings();
            }
        });

        fdReturn = new FormData();
        fdReturn.left = new FormAttachment(0, 0);
        fdReturn.top = new FormAttachment(wlReturn, margin);
        fdReturn.right = new FormAttachment(wDoMapping, -margin);
        fdReturn.bottom = new FormAttachment(wOK, -2 * margin);
        wReturn.setLayoutData(fdReturn);

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta != null) {
                    try {
                        RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

                        // Remember these fields...
                        for (int i = 0; i < row.size(); i++) {
                            inputFields.put(row.getValueMeta(i).getName(), i);
                        }
                        setComboBoxes();
                    } catch (KettleException e) {
                        logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();


        lsOK = new Listener() {
            @Override
            public void handleEvent(Event event) {
                ok();
            }
        };

        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event event) {
                cancel();
            }
        };

        lsGetLU = new Listener() {
            @Override
            public void handleEvent(Event event) {
                getUpdate();
            }
        };

        wOK.addListener(SWT.Selection, lsOK);
        wCancel.addListener(SWT.Selection, lsCancel);
        wGetLU.addListener(SWT.Selection, lsGetLU);

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wHttpurl.addSelectionListener(lsDef);
        wJdbcUrl.addSelectionListener(lsDef);
        wDatabaseName.addSelectionListener(lsDef);
        wTableName.addSelectionListener(lsDef);
        wUser.addSelectionListener(lsDef);
        wPassword.addSelectionListener(lsDef);
        wFormat.addSelectionListener(lsDef);
        wColumnSeparator.addSelectionListener(lsDef);
        wJsonPaths.addSelectionListener(lsDef);
        wMaxBytes.addSelectionListener(lsDef);
        wScanningFrequency.addSelectionListener(lsDef);
        wMaxFilterRatio.addSelectionListener(lsDef);
        wConnectTimeout.addSelectionListener(lsDef);
        wTimeout.addSelectionListener(lsDef);
        wPartialColumns.addSelectionListener(lsDef);
        wUpsertorDelete.addSelectionListener(lsDef);
        wHeaderProperties.addSelectionListener(lsDef);

        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent shellEvent) {
                cancel();
            }
        });


        // Set the shell size, based upon previous time...
        setSize();

        getData();
        setTableFieldCombo();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }
        return stepname;
    }

    /**
     * Reads in the fields from the previous steps and from the ONE next step and opens an EnterMappingDialog with this
     * information. After the user did the mapping, those information is put into the Select/Rename table.
     */
    private void generateMappings() {

        // Determine the source and target fields...
        //
        RowMetaInterface sourceFields;
        List<String> targetFields = new ArrayList<>();

        try {
            sourceFields = transMeta.getPrevStepFields(stepMeta);
        } catch (KettleException e) {
            new ErrorDialog(shell,
                    BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DoMapping.UnableToFindSourceFields.Title"),
                    BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DoMapping.UnableToFindSourceFields.Message"), e);
            return;
        }
        // refresh data
        input.setJdbcurl(wJdbcUrl.getText());
        input.setTablename(wTableName.getText());
        input.setDatabasename(wDatabaseName.getText());
        input.setUser(wUser.getText());
        input.setPassword(wPassword.getText());
        if (input.getStarRocksQueryVisitor() == null) {
            try {
                StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(input.getJdbcurl(), input.getUser(), input.getPassword());
                StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                input.setStarRocksQueryVisitor(new StarRocksQueryVisitor(jdbcConnectionProvider, input.getDatabasename(), input.getTablename()));

                targetFields = new ArrayList<>(input.getStarRocksQueryVisitor().getFieldMapping().keySet());
            } catch (Exception e) {
                new ErrorDialog(shell,
                        BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DoMapping.UnableToFindTargetFields.Title"),
                        BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DoMapping.UnableToFindTargetFields.Message"), e);
            }
        }

        String[] inputNames = new String[sourceFields.size()];
        for (int i = 0; i < sourceFields.size(); i++) {
            ValueMetaInterface value = sourceFields.getValueMeta(i);
            inputNames[i] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
        }

        // Create the existing mapping list...
        //
        List<SourceToTargetMapping> mappings = new ArrayList<>();
        StringBuilder missingSourceFields = new StringBuilder();
        StringBuilder missingTargetFields = new StringBuilder();

        int nrFields = wReturn.nrNonEmpty();
        for (int i = 0; i < nrFields; i++) {
            TableItem item = wReturn.getNonEmpty(i);
            String source = item.getText(2);
            String target = item.getText(1);

            int sourceIndex = sourceFields.indexOfValue(source);
            if (sourceIndex < 0) {
                missingSourceFields.append(Const.CR).append("   ").append(source).append(" --> ").append(target);
            }
            int targetIndex = targetFields.indexOf(target);
            if (targetIndex < 0) {
                missingTargetFields.append(Const.CR).append("   ").append(source).append(" --> ").append(target);
            }
            if (sourceIndex < 0 || targetIndex < 0) {
                continue;
            }

            SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
            mappings.add(mapping);
        }

        // show a confirm dialog if some missing field was found
        //
        if (missingSourceFields.length() > 0 || missingTargetFields.length() > 0) {

            String message = "";
            if (missingSourceFields.length() > 0) {
                message +=
                        BaseMessages.getString(
                                PKG, "StarRocksKettleConnectorDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString())
                                + Const.CR;
            }
            if (missingTargetFields.length() > 0) {
                message +=
                        BaseMessages.getString(
                                PKG, "StarRocksKettleConnectorDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString())
                                + Const.CR;
            }
            message += Const.CR;
            message +=
                    BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.DoMapping.SomeFieldsNotFoundContinue") + Const.CR;
            MessageDialog.setDefaultImage(GUIResource.getInstance().getImageSpoon());
            boolean goOn =
                    MessageDialog.openConfirm(shell, BaseMessages.getString(
                            PKG, "StarRocksKettleConnectorDialog.DoMapping.SomeFieldsNotFoundTitle"), message);
            if (!goOn) {
                return;
            }
        }
        EnterMappingDialog d = new EnterMappingDialog(StarRocksKettleConnectorDialog.this.shell, sourceFields.getFieldNames(),
                targetFields.toArray(new String[0]), mappings);
        mappings = d.open();

        // mappings == null if the user pressed cancel
        //
        if (mappings != null) {
            // Clear and re-populate!
            //
            wReturn.table.removeAll();
            wReturn.table.setItemCount(mappings.size());
            for (int i = 0; i < mappings.size(); i++) {
                SourceToTargetMapping mapping = mappings.get(i);
                TableItem item = wReturn.table.getItem(i);
                item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
                item.setText(1, targetFields.get(mapping.getTargetPosition()));
            }
            wReturn.setRowNums();
            wReturn.optWidth(true);
        }
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    public void getData() {
        if (log.isDebug()) {
            logDebug(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Log.GettingKeyInfo"));
        }
        wFormat.setText(Const.NVL(input.getFormat(), ""));
        wColumnSeparator.setText(Const.NVL((input.getColumnSeparator()), "\t"));
        wJsonPaths.setText(Const.NVL(input.getJsonpaths(), ""));
        wMaxBytes.setText(Const.NVL(String.valueOf(input.getMaxbytes()), ""));
        wScanningFrequency.setText(Const.NVL(String.valueOf(input.getScanningFrequency()), ""));
        wMaxFilterRatio.setText(Const.NVL(String.valueOf(input.getMaxFilterRatio()), ""));
        wConnectTimeout.setText(Const.NVL(String.valueOf(input.getConnecttimeout()), ""));
        wTimeout.setText(Const.NVL(String.valueOf(input.getTimeout()), ""));
        wPartialUpdate.setSelection(input.getPartialUpdate());
        wEnableUpsertDelete.setSelection(input.getEnableUpsertDelete());
        wUpsertorDelete.setText(Const.NVL(input.getUpsertOrDelete(), ""));
        wHeaderProperties.setText(Const.NVL(input.getHeaderProperties(),""));

        if (input.getPartialcolumns() != null) {
            wPartialColumns.setText(Const.NVL(String.join(",", input.getPartialcolumns()), ""));
        }

        if (input.getFieldTable() != null) {
            for (int i = 0; i < input.getFieldTable().length; i++) {
                TableItem item = wReturn.table.getItem(i);
                if (input.getFieldTable()[i] != null) {
                    item.setText(1, input.getFieldTable()[i]);
                }
                if (input.getFieldStream()[i] != null) {
                    item.setText(2, input.getFieldStream()[i]);
                }
            }
        }

        if (input.getHttpurl() != null) {
            wHttpurl.setText(String.join(";", input.getHttpurl()));
        }
        if (input.getJdbcurl() != null) {
            wJdbcUrl.setText(input.getJdbcurl());
        }
        if (input.getDatabasename() != null) {
            wDatabaseName.setText(input.getDatabasename());
        }
        if (input.getTablename() != null) {
            wTableName.setText(input.getTablename());
        }
        if (input.getUser() != null) {
            wUser.setText(input.getUser());
        }
        if (input.getPassword() != null) {
            wPassword.setText(input.getPassword());
        }

        wReturn.setRowNums();
        wReturn.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();

    }

    private void setTableFieldCombo() {
        Runnable fieldLoader = new Runnable() {
            @Override
            public void run() {
                if (!wJdbcUrl.isDisposed() && !wTableName.isDisposed() && !wDatabaseName.isDisposed() && !wUser.isDisposed() && !wPassword.isDisposed()) {
                    final String jdbcUrl = wJdbcUrl.getText(), tableName = wTableName.getText(), databaseName = wDatabaseName.getText(), user = wUser.getText(), password = wPassword.getText();

                    // Clear
                    for (ColumnInfo colInfo : tableFieldColumns) {
                        colInfo.setComboValues(new String[]{});
                    }
                    if (!Utils.isEmpty(tableName) && !Utils.isEmpty(jdbcUrl) && !Utils.isEmpty(user)) {
                        try {
                            StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(jdbcUrl, user, password);
                            StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                            StarRocksQueryVisitor starRocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnectionProvider, databaseName, tableName);

                            Map<String, StarRocksDataType> fieldMap = starRocksQueryVisitor.getFieldMapping();
                            if (null != fieldMap) {
                                String[] fieldNames = fieldMap.keySet().toArray(new String[0]);
                                for (ColumnInfo colInfo : tableFieldColumns) {
                                    colInfo.setComboValues(fieldNames);
                                }

                            }
                        } catch (Exception e) {
                            for (ColumnInfo colInfo : tableFieldColumns) {
                                colInfo.setComboValues(new String[]{});
                            }
                        }
                    }
                }
            }
        };
        shell.getDisplay().asyncExec(fieldLoader);
    }

    protected void setComboBoxes() {
        // Something was changed in the row.
        //
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        // Add the currentMeta fields...
        fields.putAll(inputFields);

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String[] fieldNames = entries.toArray(new String[entries.size()]);
        Const.sortStrings(fieldNames);
        // return fields
        ciReturn[1].setComboValues(fieldNames);
    }

    private void ok() {
        if (Utils.isEmpty(wStepname.getText())) {
            return;
        }

        // Get the information for the dialog into the input structure.
        getInfo(input);

        dispose();
    }

    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    private void getInfo(StarRocksKettleConnectorMeta inf) {
        int nrfields = wReturn.nrNonEmpty();

        inf.allocate(nrfields);

        inf.setFormat(wFormat.getText());
        inf.setColumnSeparator(wColumnSeparator.getText());
        inf.setJsonpaths(wJsonPaths.getText());
        inf.setMaxbytes(Long.valueOf(wMaxBytes.getText()));
        inf.setScanningFrequency(Long.valueOf(wScanningFrequency.getText()));
        inf.setMaxFilterRatio(Float.valueOf(wMaxFilterRatio.getText()));
        inf.setConnecttimeout(Integer.valueOf(wConnectTimeout.getText()));
        inf.setTimeout(Integer.valueOf(wTimeout.getText()));
        inf.setPartialupdate(wPartialUpdate.getSelection());
        inf.setHeaderProperties(wHeaderProperties.getText());
        if (wPartialColumns.getText() != null && wPartialColumns.getText().length() != 0) {
            inf.setPartialcolumns(wPartialColumns.getText().split(","));
        } else {
            inf.setPartialcolumns(null);
        }
        inf.setEnableupsertdelete(wEnableUpsertDelete.getSelection());
        inf.setUpsertOrDelete(wUpsertorDelete.getText());

        if (log.isDebug()) {
            logDebug(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Log.FoundFields", "" + nrfields));
        }
        //CHECKSTYLE:Indentation:OFF
        for (int i = 0; i < nrfields; i++) {
            TableItem item = wReturn.getNonEmpty(i);
            inf.getFieldTable()[i] = item.getText(1);
            inf.getFieldStream()[i] = item.getText(2);
        }
        if (wHttpurl.getText() != null && wHttpurl.getText().length() != 0) {
            inf.setHttpurl(Arrays.asList(wHttpurl.getText().split(";")));
        } else {
            inf.setHttpurl(null);
        }
        inf.setJdbcurl(wJdbcUrl.getText());
        inf.setDatabasename(wDatabaseName.getText());
        inf.setTablename(wTableName.getText());
        inf.setUser(wUser.getText());
        inf.setPassword(wPassword.getText());

        stepname = wStepname.getText();

    }

    private void getUpdate() {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields(stepname);
            if (r != null) {
                BaseStepDialog.getFieldsFromPrevious(r, wReturn, 1, new int[]{1, 2}, new int[]{}, -1, -1, null);
            }
        } catch (KettleException ke) {
            new ErrorDialog(
                    shell, BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.FailedToGetFields.DialogTitle"),
                    BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.FailedToGetFields.DialogMessage"), ke);
        }

    }
}
