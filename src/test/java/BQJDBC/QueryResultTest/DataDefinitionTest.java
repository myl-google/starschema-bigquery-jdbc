/**
 * Copyright (c) 2017, STARSCHEMA LTD.
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.

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
 */
package BQJDBC.QueryResultTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;

import net.starschema.clouddb.jdbc.BQSupportMethods;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class DataDefinitionTest {
    /** String for System independent newline */
    private static String newLine = System.getProperty("line.separator");
    /**
     * Static Connection holder
     */
    private Connection con;
    /**
     * Logger initialization
     */
    Logger logger = Logger.getLogger(this.toString());

    /**
     * Creates a new Connection to bigquery with the jdbc driver
     */
    @Before
    public void NewConnection() {
        try {
            if (con == null || !con.isValid(0)) {
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    con = DriverManager.getConnection(
                            BQSupportFuncts.constructUrlFromPropertiesFile(BQSupportFuncts
                                    .readFromPropFile(getClass().getResource(
                                            "/installedaccount.properties").getFile())),
                            BQSupportFuncts.readFromPropFile(getClass().getResource(
                                    "/installedaccount.properties").getFile()));
                } catch (Exception e) {
                    logger.debug("Failed to make connection trough the JDBC driver", e);
                }
            }
            logger.debug("Running the next test");
        } catch (SQLException e) {
            logger.fatal("Something went wrong", e);
        }
    }

    private int executeUpdate(String input) {
        int result = -1;
        try {
            result = con.createStatement().executeUpdate(input);
        } catch (SQLException e) {
            logger.error("SQLexception" + e.toString());
        }
        return result;
    }

    // Used for setup statements that aren't the ones being tested.
    private void executeUpdateRequireSuccess(String input, int expected_result) {
        int result = executeUpdate(input);
        Assert.assertEquals(expected_result, result);
    }

    private void executeQueryAndCheckResult(String query, String[][]expectation) {
        ResultSet Result = null;
        try {
            Result = con.createStatement().executeQuery(query);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertNotNull(Result);
        HelperFunctions.printer(expectation);
        try {
            Assert.assertTrue(
                    "Comparing failed in the String[][] array",
                    BQSupportMethods.comparer(expectation, BQSupportMethods.GetQueryResult(Result)));
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail(e.toString());
        }
    }

    /**
     * Test DROP TABLE statement
     */
    @Test
    public void dropTable() {
        executeUpdateRequireSuccess("drop table if exists starschema.t1;", 0);
        executeUpdateRequireSuccess("create table starschema.t1 (c1 int)", 0);

        final String drop_table = "drop table starschema.t1";
        logger.info("Running test: drop table:" + newLine + drop_table);
        int result = executeUpdate(drop_table);
        Assert.assertEquals(0, result);
    }

    @Test
    public void dropTableIfExists() {
        executeUpdateRequireSuccess("create table starschema.t1 (c1 int)", 0);

        final String drop_table = "drop table if exists starschema.t1";
        logger.info("Running test: drop table:" + newLine + drop_table);
        int result = executeUpdate(drop_table);
        Assert.assertEquals(0, result);
    }

    /**
     * Test CREATE TABLE and DROP TABLE statements
     */
    @Test
    public void createAndDropTable() {
        final String create_table = "create table starschema.t1 (c1 int, c2 char(10) null, c3 varchar(20) not null)";
        logger.info("Running test: create table:" + newLine + create_table);
        int result = executeUpdate(create_table);
        Assert.assertEquals(0, result);

        final String drop_table_if_exists = "drop table if exists starschema.t1";
        final String drop_table = "drop table starschema.t1";

        logger.info("Running test: drop table:" + newLine + drop_table);
        result = executeUpdate(drop_table);
        Assert.assertEquals(0, result);

        // Attempt to drop table that doesn't exist should fail
        logger.info("Running test: drop table:" + newLine + drop_table);
        result = executeUpdate(drop_table);
        Assert.assertEquals(-1, result);

        // Attempt to drop table that doesn't exists with IF EXISTS should succeed
        logger.info("Running test: drop table:" + newLine + drop_table_if_exists);
        result = executeUpdate(drop_table_if_exists);
        Assert.assertEquals(0, result);
    }

    /**
     * Test INSERT from SELECT statement
     */
    @Test
    public void testInsertFromSelect() {
        executeUpdateRequireSuccess("drop table if exists starschema.t1;", 0);
        executeUpdateRequireSuccess("drop table if exists starschema.t2;", 0);
        executeUpdateRequireSuccess("create table starschema.t1 (c1 int, c2 string);", 0);
        executeUpdateRequireSuccess("insert into starschema.t1 (c1, c2) values (1, 'a')", 1);
        executeUpdateRequireSuccess("insert into starschema.t1 (c1, c2) values (2, 'b')", 1);
        executeUpdateRequireSuccess("create table starschema.t2 (c3 int, c4 string, c5 int);", 0);

        // Two rows affected
        final String insert = "insert into starschema.t2 (c5, c4) select * from starschema.t1";
        logger.info("Running test: insert from select:" + newLine + insert);
        int result = executeUpdate(insert);
        Assert.assertEquals(2, result);
        executeQueryAndCheckResult("select c3, c4, c5 from starschema.t2 order by c5",
                new String[][]{{"null", "null"}, {"a", "b"}, {"1", "2"}});

        // Empty tables
        executeUpdateRequireSuccess("delete starschema.t1 where 1=1", 2);
        executeUpdateRequireSuccess("delete starschema.t2 where 1=1", 2);
        result = executeUpdate(insert);
        Assert.assertEquals(0, result);

        // Mismatched columns
        final String mismatched_columns_insert = "insert into starschema.t2 (c5) select * from starschema.t1";
        result = executeUpdate(mismatched_columns_insert);
        Assert.assertEquals(-1, result);
    }

    /**
     * Test SELECT INTO statement
     */
    @Test
    public void testSelectInto() {
        executeUpdateRequireSuccess("drop table if exists starschema.t1;", 0);
        executeUpdateRequireSuccess("create table starschema.t1 (c1 int, c2 string);", 0);
        executeUpdateRequireSuccess("insert into starschema.t1 (c1, c2) values (1, 'a')", 1);
        executeUpdateRequireSuccess("insert into starschema.t1 (c1, c2) values (2, 'b')", 1);

        // Two rows affected
        final String insert = "INTO starschema.t2 select * from starschema.t1";
        logger.info("Running test: select into:" + newLine + insert);
        int result = executeUpdate(insert);
        Assert.assertEquals(2, result);
        executeQueryAndCheckResult("select c1, c2 from starschema.t2 order by c1",
                new String[][]{{"1", "2"}, {"a", "b"}});

        // Common table expression
        final String insert_cte = "INTO starschema.t2 with cte as (select * from starschema.t1) select * from cte";
        logger.info("Running test: select into:" + newLine + insert_cte);
        result = executeUpdate(insert_cte);
        Assert.assertEquals(2, result);
        executeQueryAndCheckResult("select c1, c2 from starschema.t2 order by c1",
                new String[][]{{"1", "2"}, {"a", "b"}});

        // Existing destination table with different schema
        final String insert_one_column = "INTO starschema.t2 select c1 from starschema.t1";
        logger.info("Running test: select into:" + newLine + insert_one_column);
        result = executeUpdate(insert_one_column);
        Assert.assertEquals(2, result);
        executeQueryAndCheckResult("select * from starschema.t2 order by c1", new String[][]{{"1", "2"}});

        // Empty source table
        executeUpdateRequireSuccess("delete starschema.t1 where 1=1", 2);
        result = executeUpdate(insert);
        Assert.assertEquals(0, result);
    }
}
