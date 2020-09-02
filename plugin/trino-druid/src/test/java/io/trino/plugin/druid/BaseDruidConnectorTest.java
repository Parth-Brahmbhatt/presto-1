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
package io.trino.plugin.druid;

import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.assertions.Assert;
import org.intellij.lang.annotations.Language;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchData;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseDruidConnectorTest
        extends BaseConnectorTest
{
    protected static final String SELECT_FROM_ORDERS = "SELECT " +
            "orderdate, " +
            "orderdate AS orderdate_druid_ts, " + // Druid stores the orderdate_druid_ts column as __time column.
            "orderkey, " +
            "custkey, " +
            "orderstatus, " +
            "totalprice, " +
            "orderpriority, " +
            "clerk, " +
            "shippriority, " +
            "comment " +
            "FROM tpch.tiny.orders";

    protected static final String SELECT_FROM_LINEITEM = " SELECT " +
            "orderkey, " +
            "partkey, " +
            "suppkey, " +
            "linenumber, " +
            "quantity, " +
            "extendedprice, " +
            "discount, " +
            "tax, " +
            "returnflag, " +
            "linestatus, " +
            "shipdate, " +
            "shipdate AS shipdate_druid_ts, " +  // Druid stores the shipdate_druid_ts column as __time column.
            "commitdate, " +
            "receiptdate, " +
            "shipinstruct, " +
            "shipmode, " +
            "comment " +
            "FROM tpch.tiny.lineitem";

    protected static final String SELECT_FROM_NATION = " SELECT " +
            "nationkey, " +
            "name, " +
            "regionkey, " +
            "comment, " +
            "'1995-01-02' AS nation_druid_dummy_ts " + // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.nation";

    protected static final String SELECT_FROM_REGION = " SELECT " +
            "regionkey, " +
            "name, " +
            "comment, " +
            "'1995-01-02' AS region_druid_dummy_ts " + // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.region";

    protected static final String SELECT_FROM_PART = " SELECT " +
            "partkey, " +
            "name, " +
            "mfgr, " +
            "brand, " +
            "type, " +
            "size, " +
            "container, " +
            "retailprice, " +
            "comment, " +
            "'1995-01-02' AS part_druid_dummy_ts " + // Dummy timestamp for Druid __time column;
            "FROM tpch.tiny.part";

    protected static final String SELECT_FROM_CUSTOMER = " SELECT " +
            "custkey, " +
            "name, " +
            "address, " +
            "nationkey, " +
            "phone, " +
            "acctbal, " +
            "mktsegment, " +
            "comment, " +
            "'1995-01-02' AS customer_druid_dummy_ts " +  // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.customer";

    private static final String SELECT_SINGLE_ROW = "SELECT " +
            "CAST(1 AS DOUBLE), " +
            "CAST(1 AS REAL), " +
            "CAST(1 AS BIGINT), " +
            "'1995-01-02' AS DUMMY_TS ";

    private TestingDruidServer druidServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.druidServer = new TestingDruidServer();
        QueryRunner runner = DruidQueryRunner.createDruidQueryRunnerTpch(druidServer, ImmutableMap.of());
        copyAndIngestTpchData(runner.execute(SELECT_SINGLE_ROW), this.druidServer, "singlerow");

        // there is no create API for datasource, we just have to ingest and remove the data.
        copyAndIngestTpchData(runner.execute(SELECT_SINGLE_ROW), this.druidServer, "nodata");
        this.druidServer.dropAllSegements("nodata");

        copyAndIngestTpchData(runner.execute(SELECT_FROM_ORDERS), this.druidServer, ORDERS.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_LINEITEM), this.druidServer, LINE_ITEM.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_NATION), this.druidServer, NATION.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_REGION), this.druidServer, REGION.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_PART), this.druidServer, PART.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_CUSTOMER), this.druidServer, CUSTOMER.getTableName());
        return runner;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (druidServer != null) {
            druidServer.close();
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_INSERT:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "double", "", "")
                .build();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE druid.druid.orders (\n" +
                        "   __time timestamp(3) NOT NULL,\n" +
                        "   clerk varchar,\n" +
                        "   comment varchar,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderdate varchar,\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   orderpriority varchar,\n" +
                        "   orderstatus varchar,\n" +
                        "   shippriority bigint NOT NULL,\n" +
                        "   totalprice double NOT NULL\n" +
                        ")");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', '__time'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'",
                ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as Druid has an additional __time column
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    /**
     * This test verifies that the filtering we have in place to overcome Druid's limitation of
     * not handling the escaping of search characters like % and _, works correctly.
     * <p>
     * See {@link DruidJdbcClient#getTableHandle(ConnectorSession, SchemaTableName)} and
     * {@link DruidJdbcClient#getColumns(ConnectorSession, JdbcTableHandle)}
     */
    @Test
    public void testFilteringForTablesAndColumns()
            throws Exception
    {
        String sql = SELECT_FROM_ORDERS + " LIMIT 10";
        String datasourceA = "some_table";
        MaterializedResult materializedRows = getQueryRunner().execute(sql);
        copyAndIngestTpchData(materializedRows, druidServer, datasourceA);
        String datasourceB = "somextable";
        copyAndIngestTpchData(materializedRows, druidServer, datasourceB);

        // Assert that only columns from datsourceA are returned
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE " + datasourceA);
        Assert.assertEquals(actualColumns, expectedColumns);

        // Assert that only columns from datsourceB are returned
        expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk_x", "varchar", "", "") // String columns are reported only as varchar
                .row("comment_x", "varchar", "", "")
                .row("custkey_x", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate_x", "varchar", "", "")
                .row("orderkey_x", "bigint", "", "")
                .row("orderpriority_x", "varchar", "", "")
                .row("orderstatus_x", "varchar", "", "")
                .row("shippriority_x", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice_x", "double", "", "")
                .build();
        actualColumns = computeActual("DESCRIBE " + datasourceB);
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testAggregationPushdown()
    {
        assertThat(query("SELECT count(*) FROM orders")).isFullyPushedDown();

        // for varchar only count is pushed down
        assertThat(query("SELECT count(comment) FROM orders")).isFullyPushedDown();

        // for timestamp
        assertThat(query("SELECT count(__time) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT min(__time) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT max(__time) FROM orders")).isFullyPushedDown();

        // for double
        assertThat(query("SELECT count(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT min(totalprice) FROM orders group by custkey")).isFullyPushedDown();
        assertThat(query("SELECT max(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT sum(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT avg(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev_samp(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev_pop(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT variance(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT var_samp(totalprice) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT var_pop(totalprice) FROM orders")).isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT stddev_samp(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT stddev_pop(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT variance(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT var_samp(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT var_pop(double_col) FROM singlerow");
//        assertAggregationPushedDown("SELECT stddev(double_col) FROM nodata");
//        assertAggregationPushedDown("SELECT stddev_samp(double_col) FROM nodata");
//        assertAggregationPushedDown("SELECT stddev_pop(double_col) FROM nodata");
//        assertAggregationPushedDown("SELECT variance(double_col) FROM nodata");
//        assertAggregationPushedDown("SELECT var_samp(double_col) FROM nodata");
//        assertAggregationPushedDown("SELECT var_pop(double_col) FROM nodata");

        // for bigint
        assertThat(query("SELECT count(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT min(shippriority) FROM orders group by custkey")).isFullyPushedDown();
        assertThat(query("SELECT max(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT sum(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT avg(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev_samp(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT stddev_pop(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT variance(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT var_samp(shippriority) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT var_pop(shippriority) FROM orders")).isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev_samp(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev_pop(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT variance(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT var_samp(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT var_pop(shippriority) FROM orders").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev(bigint_col) FROM nodata").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev_samp(bigint_col) FROM nodata").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT stddev_pop(bigint_col) FROM nodata").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT variance(bigint_col) FROM nodata").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT var_samp(bigint_col) FROM nodata").isFullyPushedDown();
//        assertAggregationPushedDown("SELECT var_pop(bigint_col) FROM nodata").isFullyPushedDown();
//        assertThat(query("SELECT stddev(bigint_col) FROM singlerow")).isFullyPushedDown();
//        assertThat(query("SELECT stddev_samp(bigint_col) FROM singlerow")).isFullyPushedDown();
//        assertThat(query("SELECT stddev_pop(bigint_col) FROM singlerow")).isFullyPushedDown();
//        assertThat(query("SELECT variance(bigint_col) FROM singlerow")).isFullyPushedDown();
//        assertThat(query("SELECT var_samp(bigint_col) FROM singlerow")).isFullyPushedDown();
//        assertThat(query("SELECT var_pop(bigint_col) FROM singlerow")).isFullyPushedDown();

        //distinct
        assertThat(query("SELECT distinct shippriority,clerk FROM orders")).isFullyPushedDown();

        // instead of checking for an approximate value just checking for the plan
        assertThat(query("SELECT approx_distinct(custkey) FROM orders")).isCorrectlyPushedDown(ImmutableList.of(Optional.of(100L)));
        assertThat(query("SELECT approx_distinct(totalprice) FROM orders")).isCorrectlyPushedDown(ImmutableList.of(Optional.of(100L)));
        assertThat(query("SELECT approx_distinct(comment) FROM orders")).isCorrectlyPushedDown(ImmutableList.of(Optional.of(100L)));
        assertThat(query("SELECT approx_distinct(__time) FROM orders")).isCorrectlyPushedDown(ImmutableList.of(Optional.of(100L)));
    }
    @Test
    @Override
    public void testLimitPushDown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isNotFullyPushedDown(AggregationNode.class); // global aggregation, LIMIT removed TODO https://github.com/trinodb/trino/pull/4313
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isNotFullyPushedDown(AggregationNode.class); // TODO https://github.com/trinodb/trino/pull/4313
    }
}
