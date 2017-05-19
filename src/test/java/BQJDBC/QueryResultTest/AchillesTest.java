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

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.BQSupportMethods;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AchillesTest {
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
                                            "/achilles.properties").getFile())),
                            BQSupportFuncts.readFromPropFile(getClass().getResource(
                                    "/achilles.properties").getFile()));
                } catch (Exception e) {
                    logger.debug("Failed to make connection through the JDBC driver", e);
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
    public void runAchillesScript() {
        /*executeUpdateRequireSuccess("drop table if exists starschema.t1;", 0);
        executeUpdateRequireSuccess("create table starschema.t1 (c1 int)", 0);

        final String drop_table = "drop table starschema.t1";
        logger.info("Running test: drop table:" + newLine + drop_table);
        int result = executeUpdate(drop_table);
        Assert.assertEquals(0, result);*/
        final String replacements_path = "/Users/myl/mylSqlRender/SqlRender/inst/csv/replacementPatterns.csv";
        Path achilles_path = Paths.get("/Users/myl/achilles-scripts", "output-rendered.sql");
        String achilles_script = null;
        try {
            achilles_script = new String(Files.readAllBytes(achilles_path));
        } catch (IOException e) {
            this.logger.error("IOException" + e.toString());
            Assert.fail(e.toString());
        }
        String[] original_strings = SqlSplit.splitSql(achilles_script);
        final String translated_script = SqlTranslate.translateSqlWithPath(achilles_script, "bigquery", null,
                null, replacements_path);
        String[] translated_strings = SqlSplit.splitSql(translated_script);

        for (int i=230; i<original_strings.length; ++i) {
            final String source_sql = original_strings[i];
            final String translated_sql = translated_strings[i];
            System.out.println("\nStatement number: " + i);
            System.out.println("---------");
            System.out.println(source_sql);
            System.out.println("---------");
            System.out.println(translated_sql);
        }

    }
}
