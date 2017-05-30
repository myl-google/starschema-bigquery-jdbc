/**
 * Copyright (c) 2015, STARSCHEMA LTD.
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * <p>
 * This class is the parent of BQStatement and BQPreparedStatement
 */

package net.starschema.clouddb.jdbc;

import com.google.api.services.bigquery.model.*;
import org.antlr.runtime.tree.Tree;
import net.starschema.clouddb.jdbc.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * This class partially implements java.sql.Statement, and
 * java.sql.PreparedStatement
 *
 * @author Horváth Attila
 * @author Balazs Gunics
 *
 */
public abstract class BQStatementRoot {

    /** Reference to store the ran Query run by Executequery or Execute */
    ResultSet resset = null;

    /** String containing the context of the Project */
    String ProjectId = null;
    // Logger logger = new Logger(BQStatementRoot.class.getName());
    Logger logger = Logger.getLogger(BQStatementRoot.class.getName());

    /** Variable that stores the closed state of the statement */
    boolean closed = false;

    /** Reference for the Connection that created this Statement object */
    BQConnection connection;

    /** Variable that stores the set query timeout */
    int querytimeout = Integer.MAX_VALUE;
    /** Instance of log4j.Logger */
    /**
     * Variable stores the time an execute is made
     */
    long starttime = 0;
    /**
     * Variable that stores the max row number which can be stored in the
     * resultset
     */
    int resultMaxRowCount = Integer.MAX_VALUE - 1;

    /** Variable to Store EscapeProc state */
    boolean EscapeProc = false;

    /**
     * These Variables contain information about the type of resultset this
     * statement creates
     */
    int resultSetType;
    int resultSetConcurrency;

    /**
     * to be used with setMaxFieldSize
     */
    private int maxFieldSize = 0;

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void addBatch(String arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "addBatch(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public void cancel() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("cancel()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void clearBatch() throws SQLException {
        throw new BQSQLException("Not implemented." + "clearBatch()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void clearWarnings() throws SQLException {
        //throw new BQSQLException("Not implemented." + "clearWarnings()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Only sets closed boolean to true
     * </p>
     */
    public void close() throws SQLException {
        this.closed = true;
        if (this.resset != null) {
            this.resset.close();
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Executes the given SQL statement on BigQuery (note: it returns only 1
     * resultset). This function directly uses executeQuery function
     * </p>
     */

    public boolean execute(String arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.resset = this.executeQuery(arg0);
        this.logger.info("Executing Query: " + arg0);
        if (this.resset != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String arg0, int arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(String, int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String arg0, int[] arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(string,int[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public boolean execute(String arg0, String[] arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("execute(string,string[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int[] executeBatch() throws SQLException {
        throw new BQSQLException("Not implemented." + "executeBatch()");
    }

    /** {@inheritDoc} */

    public ResultSet executeQuery(String querySql) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.starttime = System.currentTimeMillis();
        Job referencedJob;
        // ANTLR Parsing
        BQQueryParser parser = new BQQueryParser(querySql, this.connection);
        querySql = parser.parse();

        try {
            // Gets the Job reference of the completed job with give Query
            referencedJob = BQSupportFuncts.startQuery(
                    this.connection.getBigquery(),
                    this.ProjectId,
                    querySql,
                    connection.getDataSet(),
                    this.connection.getUseLegacySql(),
                    this.connection.getMaxBillingBytes()
            );
            this.logger.info("Executing Query: " + querySql);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + querySql, e);
        }
        try {
            do {
                if (BQSupportFuncts.getQueryState(referencedJob,
                        this.connection.getBigquery(), this.ProjectId).equals(
                        "DONE")) {
                    if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE) {
                        return new BQScrollableResultSet(BQSupportFuncts.getQueryResults(
                                this.connection.getBigquery(), this.ProjectId,
                                referencedJob), this);
                    } else {
                        return new BQForwardOnlyResultSet(
                                this.connection.getBigquery(),
                                this.ProjectId.replace("__", ":").replace("_", "."),
                                referencedJob, this);
                    }
                }
                // Pause execution for half second before polling job status
                // again, to
                // reduce unnecessary calls to the BigQUery API and lower
                // overall
                // application bandwidth.
                Thread.sleep(500);
                this.logger.debug("slept for 500" + "ms, querytimeout is: "
                        + this.querytimeout + "s");
            }
            while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
            // it runs for a minimum of 1 time
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + querySql, e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // here we should kill/stop the running job, but bigquery doesn't
        // support that :(
        throw new BQSQLException(
                "Query run took more than the specified timeout");
    }

    /** {@inheritDoc} */

    public int executeUpdate(String updateSql) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.starttime = System.currentTimeMillis();

        // ANTLR Parsing
        BQQueryParser parser = new BQQueryParser(updateSql, this.connection);
        Tree dataDefinitionTree = parser.parseDataDefinition();
        if (dataDefinitionTree != null) {
            return executeDataDefinition(dataDefinitionTree, updateSql);
        }

        Job referencedJob;
        try {
            // Gets the Job reference of the completed job
            referencedJob = BQSupportFuncts.startQuery(
                    this.connection.getBigquery(),
                    this.ProjectId,
                    updateSql,
                    connection.getDataSet(),
                    false,
                    this.connection.getMaxBillingBytes()
            );
            this.logger.info("Executing Update: " + updateSql);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the update: " + updateSql, e);
        }
        try {
            do {
                Job pollJob = BQSupportFuncts.getQueryJob(referencedJob,
                        this.connection.getBigquery(), this.ProjectId);
                if (pollJob.getStatus().getState().equals("DONE")) {
                    if (pollJob.getStatus().getErrors() == null) {
                        return pollJob.getStatistics().getQuery().getNumDmlAffectedRows().intValue();
                    } else {
                        throw new BQSQLException("Error during update: " + pollJob.getStatus().getErrors().toString());
                    }
                }
                // Pause execution for half second before polling job status again, to reduce unnecessary calls to the
                // BigQuery API and lower overall application bandwidth.
                Thread.sleep(500);
                this.logger.debug("slept for 500" + "ms, querytimeout is: "
                        + this.querytimeout + "s");
            }
            while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the update: " + updateSql, e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // TODO(myl): cancel the job or set a timeout on the original request
        throw new BQSQLException(
                "Update run took more than the specified timeout");
    }

    private void verifyChildText(Tree tree, int i, String expected_text) throws SQLException {
        final String text = tree.getChild(i).getText();
        if (!text.equalsIgnoreCase(expected_text)) {
            throw new BQSQLException("Parse error: expected \"" + expected_text + "\" got \"" + text + '"');
        }
    }

    protected int executeDataDefinition(Tree tree, String updateSql) throws SQLException {
        switch (tree.getText()) {
            case "CREATETABLESTATEMENT":
                return executeCreateTable(tree);
            case "DROPTABLESTATEMENT":
                return executeDropTable(tree);
            case "TRUNCATETABLESTATEMENT":
                return executeTruncateTable(tree);
            case "INSERTFROMSELECTSTATEMENT":
                return executeInsertFromSelect(tree, updateSql);
            case "SELECTINTOSTATEMENT":
                return executeSelectIntoStatement(tree, updateSql);
        }
        throw new BQSQLFeatureNotSupportedException(updateSql);
    }

    protected int executeCreateTable(Tree tree) throws SQLException {
        TableSchema schema = new TableSchema();

        // Extract table name from the first child.
        Tree table_name_tree = tree.getChild(0);
        if (table_name_tree.getText() != "SOURCETABLE" || table_name_tree.getChildCount() != 2) {
            throw new BQSQLException("Error with table name in CREATE TABLE");
        }
        final String dataSetId = table_name_tree.getChild(0).getText();
        final String tableId = table_name_tree.getChild(1).getText();

        // Extract column definitions from the second and subsequent children.
        ArrayList<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
        for (int i = 1; i < tree.getChildCount(); ++i) {
            Tree treeChild = tree.getChild(i);
            if (treeChild.getChildCount() < 2) {
                throw new BQSQLException("Error with CREATE TABLE column definition " + i);
            }
            TableFieldSchema schema_entry = new TableFieldSchema();
            schema_entry.setName(treeChild.getChild(0).getText());
            final String type_name = treeChild.getChild(1).getText().toLowerCase();
            switch (type_name) {
                case "integer":
                case "date":
                case "datetime":
                case "timestamp":
                case "time":
                case "float":
                case "string":
                    schema_entry.setType(type_name);
                    break;
                case "char":
                case "varchar":
                case "text":
                    schema_entry.setType("string");
                    break;
                case "int":
                case "bigint":
                    schema_entry.setType("integer");
                    break;
                case "real":
                    schema_entry.setType("float");
                    break;
                default:
                    throw new BQSQLException("Invalid type in create table: " + type_name);
            }
            if (treeChild.getChildCount() == 3) {
                verifyChildText(treeChild, 2, "null");
            } else if (treeChild.getChildCount() == 4) {
                verifyChildText(treeChild, 2, "not");
                verifyChildText(treeChild, 3, "null");
                schema_entry.setMode("REQUIRED");
            }
            tableFieldSchema.add(schema_entry);
        }
        schema.setFields(tableFieldSchema);
        Table table = new Table();
        table.setSchema(schema);
        TableReference tableRef = new TableReference();
        tableRef.setDatasetId(dataSetId);
        tableRef.setProjectId(this.ProjectId);
        tableRef.setTableId(tableId);
        table.setTableReference(tableRef);
        try {
            this.connection.getBigquery().tables().insert(this.ProjectId, dataSetId, table).execute();
        } catch (IOException e) {
            throw new BQSQLException("Failed to CREATE TABLE: ", e);
        }
        return 0;
    }

    private int executeDropTable(Tree tree) throws SQLException {
        // Extract table name from the first child.
        Tree table_name_tree = tree.getChild(0);
        if (table_name_tree.getText() != "SOURCETABLE" || table_name_tree.getChildCount() != 2) {
            throw new BQSQLException("Error with table name in DROP TABLE");
        }
        final String dataSetId = table_name_tree.getChild(0).getText();
        final String tableId = table_name_tree.getChild(1).getText();

        // Check if IF EXISTS was specified
        boolean if_exists = false;
        if (tree.getChildCount() == 3 &&
                tree.getChild(1).getText().equalsIgnoreCase("if") &&
                tree.getChild(2).getText().equalsIgnoreCase("exists")) {
            if_exists = true;
        }

        // Check if the table exists
        boolean found = false;
        try {
            this.connection.getBigquery().tables().get(this.ProjectId, dataSetId, tableId).execute();
            found = true;
        } catch (IOException e) {
            // found is already false
        }

        if (!found) {
            if (if_exists) {
                // Table doesn't exist but IF EXISTS was specified.  Return success
                return 0;
            } else {
                // Table doesn't exists and IF EXISTS was not specified.  Error.
                throw new BQSQLException("Failed to DROP non-existent table: " + dataSetId + "." + tableId);
            }
        }

        try {
            this.connection.getBigquery().tables().delete(this.ProjectId, dataSetId, tableId).execute();
        } catch (IOException e) {
            throw new BQSQLException("Failed to DROP TABLE: ", e);
        }
        return 0;
    }

    /**
     *  Truncates a table (deletes all rows).
     */
    private int executeTruncateTable(Tree tree) throws SQLException {
        // Extract table name from the first child.
        Tree table_name_tree = tree.getChild(0);
        if (table_name_tree.getText() != "SOURCETABLE" || table_name_tree.getChildCount() != 2) {
            throw new BQSQLException("Error with table name in TRUNCATE TABLE");
        }
        final String dataSetId = table_name_tree.getChild(0).getText();
        final String tableId = table_name_tree.getChild(1).getText();

        Table table = null;
        try {
            table = this.connection.getBigquery().tables().get(this.ProjectId, dataSetId, tableId).execute();
        } catch (IOException e) {
            throw new BQSQLException("Table not found for TRUNCATE: ", e);
        }
        final int numRows = table.getNumRows().intValue();

        try {
            this.connection.getBigquery().tables().delete(this.ProjectId, dataSetId, tableId).execute();
        } catch (IOException e) {
            throw new BQSQLException("Failed to TRUNCATE TABLE: ", e);
        }

        try {
            Table table_copy = new Table();
            table_copy.setSchema(table.getSchema());
            TableReference tableRef = new TableReference();
            tableRef.setDatasetId(dataSetId);
            tableRef.setProjectId(this.ProjectId);
            tableRef.setTableId(tableId);
            table_copy.setTableReference(tableRef);
            this.connection.getBigquery().tables().insert(this.ProjectId, dataSetId, table_copy).execute();
        } catch (IOException e) {
            throw new BQSQLException("Failed to TRUNCATE TABLE: ", e);
        }
        return numRows;
    }

    /**
     *  Runs a select statement directs the output to the specified destination table. Either overwrites
     *  or appends to the destination table based on the value of destinationAppend.
     */
    private void executeSelectWithDestination(String selectQuery, String destinationDataSet, String destinationTableId,
                                              boolean destinationAppend) throws SQLException {
        Job referencedJob;
        try {
            referencedJob = BQSupportFuncts.startQueryWithDestination(
                    this.connection.getBigquery(),
                    this.ProjectId,
                    selectQuery,
                    connection.getDataSet(),
                    false,
                    this.connection.getMaxBillingBytes(),
                    destinationDataSet,
                    destinationTableId,
                    destinationAppend
            );
            this.logger.info("Executing Query: " + selectQuery);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + selectQuery, e);
        }
        try {
            do {
                Job pollJob = BQSupportFuncts.getQueryJob(referencedJob,
                        this.connection.getBigquery(), this.ProjectId);
                if (pollJob.getStatus().getState().equals("DONE")) {
                    if (pollJob.getStatus().getErrors() == null) {
                        return;
                    } else {
                        throw new BQSQLException("Error during update: " + pollJob.getStatus().getErrors().toString());
                    }
                }
                Thread.sleep(500);
                this.logger.debug("slept for 500ms, querytimeout is: " + this.querytimeout + "s");
            } while (System.currentTimeMillis() - this.starttime <= (long) this.querytimeout * 1000);
        } catch (IOException e) {
            throw new BQSQLException("Something went wrong with the query: " + selectQuery, e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        throw new BQSQLException("Query run took more than the specified timeout");
    }

    /**
     *  Looks up the given table using the bigquery API and returns the schema of the columns.
     */
    private List<TableFieldSchema> getTableFields(String dataSet, String tableId)
            throws BQSQLException {
        try {
            final Table table = this.connection.getBigquery().tables().get(this.ProjectId, dataSet, tableId).execute();
            return table.getSchema().getFields();
        } catch (IOException e) {
            throw new BQSQLException("Failed to lookup table: " + dataSet + "." + tableId, e);
        }
    }

    /**
     *  Looks up the table using the bigquery API and returns the number of rows.
     */
    private int getNumRows(String dataSet, String tableId) throws BQSQLException {
        try {
            Table table = this.connection.getBigquery().tables().get(this.ProjectId, dataSet, tableId).execute();
            return table.getNumRows().intValue();
        } catch (IOException e) {
            throw new BQSQLException("Failed to lookup table: " + dataSet + "." + tableId, e);
        }
    }

    /**
     *  Converts legacy types to standard sql types
     */
    private String getStandardTypeFromLegacyType(String type) {
        switch(type.toLowerCase()) {
            case "integer":
                return "int64";
            case "float":
                return "float64";
            default:
                return type;
        }
    }

    /**
     *  Runs an INSERT from SELECT statement in two parts.  First we execute the SELECT and direct the
     *  results to a temp table.  Then we select from the temp table with columns named as specified
     *  in the INSERT list and direct the result to append to the final destination table.
     */
    private int executeInsertFromSelect(Tree tree, String updateSql) throws SQLException {
        // Extract table name from the first child.
        Tree table_name_tree = tree.getChild(0);
        if (table_name_tree.getText() != "SOURCETABLE" || table_name_tree.getChildCount() != 2) {
            throw new BQSQLException("Error with table name in INSERT from SELECT");
        }
        final String dataSetId = table_name_tree.getChild(0).getText();
        final String tableId = table_name_tree.getChild(1).getText();

        // Extract the select statement part
        final Tree selectNode = tree.getChild(1).getChild(0);
        if (!selectNode.getText().equalsIgnoreCase("select")) {
            throw new BQSQLException("Error with table name in INSERT from SELECT");
        }
        final String selectQuery = updateSql.substring(selectNode.getCharPositionInLine());

        // Find the destination column names
        ArrayList<String> declared_dest_column_names = new ArrayList<String>();
        for (int i=2; i < tree.getChildCount(); ++i) {
            declared_dest_column_names.add(tree.getChild(i).getText());
        }

        // Execute first with a temporary table as the destination
        Random random = new Random();
        final String tempDataSet = "temp";
        final String tempTableid = "t" + (random.nextLong() & 0xffffffffL);  // generate positive integer
        executeSelectWithDestination(selectQuery, tempDataSet, tempTableid, false);

        // Find the column of the temporary table and check that there are many as expected.
        List<TableFieldSchema> temp_columns = getTableFields(tempDataSet, tempTableid);
        if (temp_columns.size() != declared_dest_column_names.size()) {
            throw new BQSQLException("Mismatch in declared and actual columns executing INSERT from SELECT");
        }

        // Find the column names of the destination table.
        List<TableFieldSchema> dest_columns = getTableFields(dataSetId, tableId);

        // Construct a select list to populate the destination table.
        String temp_select_list = "";
        for (TableFieldSchema dest_column : dest_columns) {
            for (int i = 0; i < declared_dest_column_names.size(); ++i) {
                if (declared_dest_column_names.get(i).equals(dest_column.getName())) {
                    temp_select_list = temp_select_list + "," +
                            "cast(" + temp_columns.get(i).getName() + " as " +
                            getStandardTypeFromLegacyType(dest_column.getType()) +
                            ") as " + dest_column.getName();
                    break;
                }
            }
        }

        // Execute a second query over the temp table with the final table as the destination
        final String tempSelectQuery = "select " + temp_select_list.substring(1) + " from " + tempDataSet +
                "." + tempTableid;
        executeSelectWithDestination(tempSelectQuery, dataSetId, tableId, true);

        return getNumRows(tempDataSet, tempTableid);
    }

    /**
     *  Runs a SELECT INTO statement in two parts and overwrites the destination table.
     */
    private int executeSelectIntoStatement(Tree tree, String updateSql) throws SQLException {
        // Extract table name from the first child.
        Tree table_name_tree = tree.getChild(0);
        if (table_name_tree.getText() != "SOURCETABLE" || table_name_tree.getChildCount() != 2) {
            throw new BQSQLException("Error with table name in INSERT from SELECT");
        }
        final String dataSetId = table_name_tree.getChild(0).getText();
        final String tableId = table_name_tree.getChild(1).getText();

        // Extract and execute the query
        final String selectQuery = updateSql.substring(tree.getChild(1).getCharPositionInLine());
        executeSelectWithDestination(selectQuery, dataSetId, tableId, false);

        return getNumRows(dataSetId, tableId);
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String arg0, int arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("executeUpdate(String,int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "executeUpdate(string,int[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        throw new BQSQLFeatureNotSupportedException(
                "execute(update(string,string[])");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the connection that made the object.
     * </p>
     *
     * @returns connection
     */
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Fetch direction is unknown.
     * </p>
     *
     * @return FETCH_UNKNOWN
     */

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int getFetchSize() throws SQLException {
        throw new BQSQLException("Not implemented." + "getFetchSize()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public ResultSet getGeneratedKeys() throws SQLException {
        throw new BQSQLFeatureNotSupportedException("getGeneratedKeys()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * We store it in an array which indexed through, int
     * </p>
     * We could return Integer.MAX_VALUE too, but i don't think we could get
     * that much row.
     *
     * @return 0 -
     */

    public int getMaxRows() throws SQLException {
        return this.resultMaxRowCount;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet, since Bigquery Does not support precompiled sql
     * </p>
     *
     * @throws BQSQLException
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new BQSQLException(new SQLFeatureNotSupportedException(
                "getMetaData()"));
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Multiple result sets are not supported currently.
     * </p>
     *
     * @return false;
     */

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Multiple result sets are not supported currently. we check that the
     * result set is open, the parameter is acceptable, and close our current
     * resultset or throw a FeatureNotSupportedException
     * </p>
     *
     * @param current
     *            - one of the following Statement constants indicating what
     *            should happen to current ResultSet objects obtained using the
     *            method getResultSet: Statement.CLOSE_CURRENT_RESULT,
     *            Statement.KEEP_CURRENT_RESULT, or Statement.CLOSE_ALL_RESULTS
     * @throws BQSQLException
     */

    public boolean getMoreResults(int current) throws SQLException {
        if (this.closed) {
            throw new BQSQLException("Statement is closed.");
        }
        if (current == Statement.CLOSE_CURRENT_RESULT
                || current == Statement.KEEP_CURRENT_RESULT
                || current == Statement.CLOSE_ALL_RESULTS) {

            if (BQDatabaseMetadata.multipleOpenResultsSupported
                    && (current == Statement.KEEP_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS)) {
                throw new BQSQLFeatureNotSupportedException();
            }
            // Statement.CLOSE_CURRENT_RESULT
            this.close();
            return false;
        } else {
            throw new BQSQLException("Wrong parameter.");
        }
    }

    /** {@inheritDoc} */

    public int getQueryTimeout() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (this.starttime == 0) {
            return 0;
        }
        if (this.querytimeout == Integer.MAX_VALUE) {
            return 0;
        } else {
            if (System.currentTimeMillis() - this.starttime > this.querytimeout) {
                throw new BQSQLException("Time is over");
            }
            return this.querytimeout;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Gives back reultset stored in resset
     * </p>
     */

    public ResultSet getResultSet() throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (this.resset != null) {
            return this.resset;
        } else {
            return null;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * The driver is read only currently.
     * </p>
     *
     * @return CONCUR_READ_ONLY
     */

    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Read only mode, no commit.
     * </p>
     *
     * @return CLOSE_CURSORS_AT_COMMIT
     */

    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        // TODO
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Updates and deletes not supported.
     * </p>
     *
     * @return TYPE_SCROLL_INSENSITIVE
     */

    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Result will be a ResultSet object.
     * </p>
     *
     * @return -1
     */

    public int getUpdateCount() throws SQLException {
        return -1;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * returns null
     * </p>
     *
     * @return null
     */

    public SQLWarning getWarnings() throws SQLException {
        return null;
        // TODO Implement Warning Handling
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Returns the value of boolean closed
     * </p>
     */
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public boolean isPoolable() throws SQLException {
        throw new BQSQLException("Not implemented." + "isPoolable()");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @return false
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * FeatureNotSupportedExceptionjpg
     * </p>
     *
     * @throws BQSQLFeatureNotSupportedException
     */

    public void setCursorName(String arg0) throws SQLException {
        throw new BQSQLFeatureNotSupportedException("setCursorName(string)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Now Only setf this.EscapeProc to arg0
     * </p>
     */

    public void setEscapeProcessing(boolean arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        this.EscapeProc = arg0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */

    public void setFetchDirection(int arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "setFetchDirection(int)");
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Does nothing
     * </p>
     *
     * @throws BQSQLException
     */
    public void setFetchSize(int arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("Statement closed");
        }
    }

    /**
     * <p>
     * Sets the limit for the maximum number of bytes in a ResultSet column storing
     * character or binary values to the given number of bytes. This limit applies only
     * to BINARY, VARBINARY, LONGVARBINARY, CHAR, VARCHAR, and LONGVARCHAR fields.
     * If the limit is exceeded, the excess data is silently discarded. For maximum
     * portability, use values greater than 256.
     * </p>
     *
     * @throws BQSQLException
     */
    public void setMaxFieldSize(int arg0) throws SQLException {
        this.maxFieldSize = arg0;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * arg0 == 0 ? arg0 : Integer.MAX_VALUE - 1
     * </p>
     *
     * @throws BQSQLException
     */
    public void setMaxRows(int arg0) throws SQLException {
        this.resultMaxRowCount = arg0 == 0 ? arg0 : Integer.MAX_VALUE - 1;
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Not implemented yet.
     * </p>
     *
     * @throws BQSQLException
     */
    public void setPoolable(boolean arg0) throws SQLException {
        throw new BQSQLException("Not implemented." + "setPoolable(bool)");
    }

    /** {@inheritDoc} */
    public void setQueryTimeout(int arg0) throws SQLException {
        if (this.isClosed()) {
            throw new BQSQLException("This Statement is Closed");
        }
        if (arg0 == 0) {
            this.querytimeout = Integer.MAX_VALUE;
        } else {
            this.querytimeout = arg0;
        }
    }

    /**
     * <p>
     * <h1>Implementation Details:</h1><br>
     * Always throws SQLException
     * </p>
     *
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new BQSQLException("not found");
    }
}
