/**
 * Copyright (c) 2015, STARSCHEMA LTD.
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
package BQJDBC.DataManipulationTest;

import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//import net.starschema.clouddb.bqjdbc.logging.Logger;

/**
 * This Junit tests if DML statements work as expected
 *
 * @author Matthew Young-Lai
 */
public class DataManipulationTest {

    private static java.sql.Connection con = null;
    //Logger logger = new Logger(DataManipulationTest.class.getName());
    Logger logger = Logger.getLogger(DataManipulationTest.class.getName());

    /**
     * Makes a new Bigquery Connection to Hardcoded URL and gives back the
     * Connection to static con member.
     */
    @Before
    public void NewConnection() {
        try {
            if (DataManipulationTest.con == null || !DataManipulationTest.con.isValid(0)) {

                this.logger.info("Testing the JDBC driver");
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    DataManipulationTest.con = DriverManager
                            .getConnection(
                                    BQSupportFuncts
                                            .constructUrlFromPropertiesFile(BQSupportFuncts
                                                    .readFromPropFile(getClass().getResource(
                                                            "/serviceaccount.properties").getFile())),
                                    BQSupportFuncts
                                            .readFromPropFile(getClass().getResource(
                                                    "/serviceaccount.properties").getFile()));
                } catch (Exception e) {
                    this.logger.error("Error in connection" + e.toString());
                    Assert.fail("General Exception:" + e.toString());
                }
                this.logger.info(((BQConnection) DataManipulationTest.con)
                        .getURLPART());
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void logStatement(String sql) {
        this.logger.info("Running statement:" + sql);
    }

    @Test
    public void InsertTest() {
        final String sql = "insert into starschema.test(col) values ('a')";

        this.logger.info("Test number: InsertTest");
        logStatement(sql);

        int result = 0;
        try {
            Statement stm = con.createStatement();
            result = stm.executeUpdate(sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertEquals(result, 1);
    }

    @Test
    public void InsertUpdateDeleteTest() {
        int result = 0;
        this.logger.info("Test number: InsertUpdateDeleteTest");

        final String insert_sql = "insert into starschema.test(col) values ('b')";
        logStatement(insert_sql);
        try {
            Statement stm = con.createStatement();
            result = stm.executeUpdate(insert_sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertEquals(1, result);

        final String update_sql = "update starschema.test set col='c' where col='b'";
        logStatement(update_sql);
        try {
            Statement stm = con.createStatement();
            result = stm.executeUpdate(update_sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertEquals(1, result);

        final String delete_sql = "delete starschema.test where col='c'";
        logStatement(delete_sql);
        try {
            Statement stm = con.createStatement();
            result = stm.executeUpdate(delete_sql);
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            Assert.fail("SQLException" + e.toString());
        }
        Assert.assertEquals(1, result );
    }
}

