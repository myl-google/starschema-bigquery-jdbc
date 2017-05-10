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

    /**
     * Test DROP TABLE statement
     */
    @Test
    public void dropTable() {
        final String create_table = "create table starschema.t1 (c1 int)";
        logger.info("Running test: create table:" + newLine + create_table);
        int result = executeUpdate(create_table);
        Assert.assertEquals(0, result);

        final String drop_table = "drop table starschema.t1";
        logger.info("Running test: drop table:" + newLine + drop_table);
        result = executeUpdate(drop_table);
        Assert.assertEquals(0, result);
    }

    @Test
    public void dropTableIfExists() {
        final String create_table = "create table starschema.t1 (c1 int)";
        logger.info("Running test: create table:" + newLine + create_table);
        int result = executeUpdate(create_table);
        Assert.assertEquals(0, result);

        final String drop_table = "drop table if exists starschema.t1";
        logger.info("Running test: drop table:" + newLine + drop_table);
        result = executeUpdate(drop_table);
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
}
