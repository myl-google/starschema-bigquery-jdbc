package BQJDBC.QueryResultTest;

import BQJDBC.QueryResultTest.QueryResultTest;
import junit.framework.Assert;
import net.starschema.clouddb.jdbc.BQConnection;
import net.starschema.clouddb.jdbc.BQSupportFuncts;
import net.starschema.clouddb.jdbc.BQSupportMethods;
import org.apache.log4j.Logger;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BaseTest {

    /** String for System independent newline */
    protected static String newLine = System.getProperty("line.separator");

    protected static java.sql.Connection con = null;

    //Logger logger = new Logger(QueryResultTest.class.getName());
    protected Logger logger = Logger.getLogger(QueryResultTest.class.getName());

    /**
     * Makes a new Bigquery Connection to Hardcoded URL and gives back the
     * Connection to static con member.
     */
    @Before
    public void NewConnection() {
        try {
            if (con == null || !con.isValid(0)) {

                this.logger.info("Testing the JDBC driver");
                try {
                    Class.forName("net.starschema.clouddb.jdbc.BQDriver");
                    con = DriverManager
                            .getConnection(
                                    BQSupportFuncts
                                            .constructUrlFromPropertiesFile(BQSupportFuncts
                                                    .readFromPropFile(getClass().getResource("/serviceaccount.properties").getFile())),
                                    BQSupportFuncts
                                            .readFromPropFile(getClass().getResource("/serviceaccount.properties").getFile()));
                } catch (Exception e) {
                    this.logger.error("Error in connection" + e.toString());
                    Assert.fail("General Exception:" + e.toString());
                }
                this.logger.info(((BQConnection) con).getURLPART());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected int executeUpdate(String sql, boolean prepared) {
        this.logger.info("Running statement:" + sql);
        int result = -1;
        try {
            if (prepared) {
                result = con.prepareStatement(sql).executeUpdate();
            } else {
                result = con.createStatement().executeUpdate(sql);
            }
        } catch (SQLException e) {
            this.logger.error("SQLexception" + e.toString());
            //Assert.fail("SQLException" + e.toString());
        }
        return result;
    }

    // Used for setup statements that aren't the ones being tested.
    protected void executeUpdateRequireSuccess(String input, int expected_result) {
        int result = executeUpdate(input, false);
        Assert.assertEquals(expected_result, result);
    }

    protected void executeQueryAndCheckResult(String query, String[][] expectation) {
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
}
