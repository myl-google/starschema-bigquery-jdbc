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
package BQJDBC.QueryResultTest;

import junit.framework.Assert;
import org.junit.Test;

import java.sql.SQLException;


/**
 * This Junit tests if DML statements work as expected
 *
 * @author Matthew Young-Lai
 */
public class DataManipulationTest extends BaseTest {
    @Test
    public void InsertTest() {
        final String sql = "insert into starschema.test(col) values ('a')";

        this.logger.info("Test number: InsertTest");
        int result = executeUpdate(sql, false);
        Assert.assertEquals(result, 1);
    }

    @Test
    public void PreparedInsertTest() {
        final String sql = "insert into starschema.test(col) values ('a')";

        this.logger.info("Test number: PreparedInsertTest");
        int result = executeUpdate(sql, true);
        Assert.assertEquals(1, result);
    }

    @Test
    public void FailedInsertTest() {
        final String sql = "insert into starschema.missingtable(col) values ('a')";

        this.logger.info("Test number: FailedInsertTest");
        int result = 0;
        String error_message = null;
        try {
            result = con.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            error_message = e.toString();
        }
        Assert.assertEquals(0, result);
        Assert.assertNotNull(error_message);
    }

    @Test
    public void InsertUpdateDeleteTest() {
        int result = 0;
        this.logger.info("Test number: InsertUpdateDeleteTest");

        final String insert_sql = "insert into starschema.test(col) values ('b')";
        result = executeUpdate(insert_sql, false);
        Assert.assertEquals(1, result);

        final String update_sql = "update starschema.test set col='c' where col='b'";
        result = executeUpdate(update_sql, false);
        Assert.assertEquals(1, result);

        final String delete_sql = "delete starschema.test where col='c'";
        result = executeUpdate(delete_sql, false);
        Assert.assertEquals(1, result );
    }

    @Test
    public void NextvalTest() {
        int result = 0;
        this.logger.info("Test number: NEXTVAL");

        executeUpdate("delete starschema.sequence where sequence_name='test_sequence'", false);
        executeUpdateRequireSuccess("insert starschema.sequence (sequence_name, next_value) values ('test_sequence', 500)", 1);

        executeQueryAndCheckResult("select nextval('test_sequence')", new String[][]{{"501"}});
        executeQueryAndCheckResult("select nextval('test_sequence')", new String[][]{{"502"}});
    }
}

