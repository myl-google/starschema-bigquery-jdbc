package BQJDBC.QueryResultTest;

import net.starschema.clouddb.jdbc.BQStatementRoot;
import org.junit.Assert;
import org.junit.Test;

public class BQStatementRootTest {

    // exposes protected member normalizeDataDefinitionForParsing
    public class FakeBqStatement extends BQStatementRoot {
        public String getNormalizedUpdateSql(String updateSql) {
            return normalizeDataDefinitionForParsing(updateSql);
        }
    }

    @Test
    public void removeUpdateSqlComments(){
        FakeBqStatement bqStatementRoot = new FakeBqStatement();
        String normalized = bqStatementRoot.getNormalizedUpdateSql("update t1 -- abc\nset c1=0");
        Assert.assertEquals("update t1 set c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("delete from t1 -- abc\nwhere c1=0");
        Assert.assertEquals("delete from t1 where c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("insert into t1(c1) -- abc\nvalues('a')");
        Assert.assertEquals("insert into t1(c1) values('a')", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("create table t1 -- abc\n(c1 int)");
        Assert.assertEquals("create table t1 (c1 int)", normalized);
    }

    @Test
    public void removeUpdateSqlCommentsMultiLine(){
        FakeBqStatement bqStatementRoot = new FakeBqStatement();

        /* single line */
        String normalized = bqStatementRoot.getNormalizedUpdateSql("update t1 /* abc */set c1=0");
        Assert.assertEquals("update t1 set c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("delete from t1 /* abc */where c1=0");
        Assert.assertEquals("delete from t1 where c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("insert into t1(c1) /* abc */values('a')");
        Assert.assertEquals("insert into t1(c1) values('a')", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("create table t1 /* abc */(c1 int)");
        Assert.assertEquals("create table t1 (c1 int)", normalized);

        /*
         multiple lines
         */
        normalized = bqStatementRoot.getNormalizedUpdateSql("update t1 /*\nabc\n*/set c1=0");
        Assert.assertEquals("update t1 set c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("delete from t1 /*\nabc\n*/where c1=0");
        Assert.assertEquals("delete from t1 where c1=0", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("insert into t1(c1) /*\nabc\n*/values('a')");
        Assert.assertEquals("insert into t1(c1) values('a')", normalized);

        normalized = bqStatementRoot.getNormalizedUpdateSql("create table t1 /*\nabc\n*/(c1 int)");
        Assert.assertEquals("create table t1 (c1 int)", normalized);

        String comment = "/************************\n\n" +
                "Standardized vocabulary\n\n" +
                "************************/\n\n\n";
        String createConceptTable = "create table concept (\n" +
                "  concept_id\t\t\t    integer\t\t\t  not null ,\n" +
                "  concept_name\t\t\t  varchar(255)\tnot null ,\n" +
                "  domain_id\t\t\t\t    varchar(20)\t\tnot null ,\n" +
                "  vocabulary_id\t\t\t  varchar(20)\t\tnot null ,\n" +
                "  concept_class_id\t\tvarchar(20)\t\tnot null ,\n" +
                "  standard_concept\t\tvarchar(1)\t\tnull ,\n" +
                "  concept_code\t\t\t  varchar(50)\t\tnot null ,\n" +
                "  valid_start_date\t\tdate\t\t\t    not null ,\n" +
                "  valid_end_date\t\t  date\t\t\t    not null ,\n" +
                "  invalid_reason\t\t  varchar(1)\t\tnull\n" +
                ")";
        String updateSql = comment + createConceptTable;
        normalized = bqStatementRoot.getNormalizedUpdateSql(updateSql);
        Assert.assertEquals("\n\n\n"+createConceptTable, normalized);
    }
}
