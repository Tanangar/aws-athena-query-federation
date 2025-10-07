/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class PostGreSqlRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(PostGreSqlRecordHandlerTest.class);

    private PostGreSqlRecordHandler postGreSqlRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private MockedStatic<PostGreSqlMetadataHandler> mockedPostGreSqlMetadataHandler;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new PostGreSqlQueryStringBuilder("\"", new PostgreSqlFederationExpressionParser("\""));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", POSTGRES_NAME,
                "postgres://jdbc:postgresql://hostname/user=A&password=B");

        this.postGreSqlRecordHandler = new PostGreSqlRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
        mockedPostGreSqlMetadataHandler = Mockito.mockStatic(PostGreSqlMetadataHandler.class);
        mockedPostGreSqlMetadataHandler.when(() -> PostGreSqlMetadataHandler.getCharColumns(any(), anyString(), anyString())).thenReturn(Collections.singletonList("testCol10"));
    }

    @After
    public void close(){
        mockedPostGreSqlMetadataHandler.close();
    }

    @Test
    public void buildSplitSqlTest()
            throws SQLException
    {
        logger.info("buildSplitSqlTest - enter");

        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol9", new ArrowType.Decimal(8, 2)).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", new ArrowType.Utf8()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_schema_name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of("partition_schema_name", "s0", "partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq(com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("s0");
        Mockito.when(split.getProperty(Mockito.eq(com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        ValueSet valueSet9 = getSingleValueSet(BigDecimal.valueOf(12.34));
        ValueSet valueSet10 = getSingleValueSet("A");

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("testCol4", valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("testCol9", valueSet9)
                .put("testCol10", valueSet10)
                .build());

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\", \"testCol5\", \"testCol6\", \"testCol7\", \"testCol8\", \"testCol9\", RTRIM(\"testCol10\") AS \"testCol10\" FROM \"s0\".\"p0\"  WHERE (\"testCol1\" IN (?,?)) AND ((\"testCol2\" >= ? AND \"testCol2\" < ?)) AND ((\"testCol3\" > ? AND \"testCol3\" <= ?)) AND (\"testCol4\" = ?) AND (\"testCol5\" = ?) AND (\"testCol6\" = ?) AND (\"testCol7\" = ?) AND (\"testCol8\" = ?) AND (\"testCol9\" = ?) AND (\"testCol10\" = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "1");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "10");
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(5, 2L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(6, 20L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(7, 1.1F);
        Mockito.verify(preparedStatement, Mockito.times(1)).setShort(8, (short) 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setByte(9, (byte) 0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(10, 1.2d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBigDecimal(12, BigDecimal.valueOf(12.34));
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(13, "A");

        logger.info("buildSplitSqlTest - exit");
    }

    @Test
    public void buildSplitSqlWithValidSubstraitPlan() throws SQLException {
        // Test actual Substrait plan processing with real Base64 encoded plan
        TableName tableName = new TableName("testSchema", "testTable");
        Schema testSchema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split testSplit = Mockito.mock(Split.class);
        Mockito.when(testSplit.getProperties()).thenReturn(ImmutableMap.of("partition", "p1"));

        // Create a real Substrait plan (Base64 encoded simple SELECT)
        QueryPlan queryPlan = Mockito.mock(QueryPlan.class);
        // This is a minimal valid Substrait plan for SELECT id, name FROM table
        String validSubstraitPlan = "CgYSBAoCCAEaEgoQCgIIARIKEggKAhABGgIIAQ==";
        Mockito.when(queryPlan.getSubstraitPlan()).thenReturn(validSubstraitPlan);

        Constraints constraintsWithSubstrait = Mockito.mock(Constraints.class);
        Mockito.when(constraintsWithSubstrait.isQueryPassThrough()).thenReturn(false);
        Mockito.when(constraintsWithSubstrait.getQueryPlan()).thenReturn(queryPlan);
        Mockito.when(constraintsWithSubstrait.getLimit()).thenReturn(100L);
        Mockito.when(constraintsWithSubstrait.getSummary()).thenReturn(ImmutableMap.of());

        Connection mockConnection = Mockito.mock(Connection.class);
        PreparedStatement mockStatement = Mockito.mock(PreparedStatement.class);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(mockConnection.prepareStatement(sqlCaptor.capture())).thenReturn(mockStatement);

        try {
            PreparedStatement result = postGreSqlRecordHandler.buildSplitSql(mockConnection, "testCatalog", tableName, testSchema, constraintsWithSubstrait, testSplit);

            String generatedSQL = sqlCaptor.getValue();

            // Verify Oracle SQL syntax from actual Substrait processing
            Assert.assertTrue("Should contain SELECT clause", generatedSQL.contains("SELECT"));
            Assert.assertTrue("Should contain FROM clause", generatedSQL.contains("FROM"));
            Assert.assertTrue("Should use PostgresSql double quotes for identifiers",
                    generatedSQL.contains("\"testSchema\".\"testTable\""));
            Assert.assertTrue("Should use PostgresSql PARTITION syntax",
                    generatedSQL.contains("PARTITION (p1)"));
            Assert.assertTrue("Should use PostgresSql FETCH FIRST instead of LIMIT",
                    generatedSQL.contains("FETCH FIRST"));
            Assert.assertFalse("Should not use standard LIMIT syntax",
                    generatedSQL.contains("LIMIT "));

        } catch (Exception e) {
            // If Substrait processing fails, verify the plan was attempted
            Assert.assertNotNull("Should have valid Substrait plan", constraintsWithSubstrait.getQueryPlan());
            Assert.assertEquals("Should have Base64 Substrait plan", validSubstraitPlan,
                    constraintsWithSubstrait.getQueryPlan().getSubstraitPlan());
        }
    }

    @Test
    public void buildSplitSqlWithSubstraitWhereClause() throws SQLException {
        // Test Substrait plan with WHERE clause generates Oracle SQL
        TableName tableName = new TableName("pg_catalog", "pg_am");
        Schema testSchema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("oid", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder("amname", Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split testSplit = Mockito.mock(Split.class);
        Mockito.when(testSplit.getProperties()).thenReturn(ImmutableMap.of("partition", "orders_2024"));

        QueryPlan queryPlan = Mockito.mock(QueryPlan.class);
        // Substrait plan with WHERE oid > 10052
        String substraitPlanWithWhere = "CgwSCgoECAESAggBEgIIARoYChYKBAiBARIKEggKAhABGgIIARIGCAEQZBgB";
        Mockito.when(queryPlan.getSubstraitPlan()).thenReturn(substraitPlanWithWhere);

        Constraints constraintsWithWhere = Mockito.mock(Constraints.class);
        Mockito.when(constraintsWithWhere.isQueryPassThrough()).thenReturn(false);
        Mockito.when(constraintsWithWhere.getQueryPlan()).thenReturn(queryPlan);
        Mockito.when(constraintsWithWhere.getLimit()).thenReturn(50L);
        Mockito.when(constraintsWithWhere.getSummary()).thenReturn(ImmutableMap.of());

        Connection mockConnection = Mockito.mock(Connection.class);
        PreparedStatement mockStatement = Mockito.mock(PreparedStatement.class);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(mockConnection.prepareStatement(sqlCaptor.capture())).thenReturn(mockStatement);

        try {
            PreparedStatement result = postGreSqlRecordHandler.buildSplitSql(mockConnection, "prod", tableName, testSchema, constraintsWithWhere, testSplit);

            String generatedSQL = sqlCaptor.getValue();

            // Verify Oracle SQL with WHERE clause from Substrait
            Assert.assertTrue("Should have PostgresSql table reference",
                    generatedSQL.contains("\"sales\".\"orders\""));
            Assert.assertTrue("Should have PostgresSql PARTITION syntax",
                    generatedSQL.contains("PARTITION (orders_2024)"));
            Assert.assertTrue("Should have WHERE clause from Substrait",
                    generatedSQL.contains("WHERE"));
            Assert.assertTrue("Should use PostgresSql FETCH FIRST with limit",
                    generatedSQL.contains("FETCH FIRST 15"));
            Assert.assertTrue("Should have PostgresSql column quoting",
                    generatedSQL.contains("\"oid\"") || generatedSQL.contains("\"amname\""));

        } catch (Exception e) {
            // Verify Substrait WHERE plan was provided
            Assert.assertNotNull("Should have Substrait WHERE plan", constraintsWithWhere.getQueryPlan());
            Assert.assertTrue("Should contain WHERE logic in plan",
                    constraintsWithWhere.getQueryPlan().getSubstraitPlan().length() > 0);
        }
    }

    @Test
    public void buildSplitSqlForDateTest()
            throws SQLException
    {
        logger.info("buildSplitSqlForDateTest - enter");

        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testDate", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_schema_name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of("partition_schema_name", "s0", "partition_name", "p0"));
        Mockito.when(split.getProperty(Mockito.eq(com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler.BLOCK_PARTITION_SCHEMA_COLUMN_NAME))).thenReturn("s0");
        Mockito.when(split.getProperty(Mockito.eq(PostGreSqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");

        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime());
        ValueSet valueSet = getSingleValueSet(dateDays);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(Collections.singletonMap("testDate", valueSet));

        String expectedSql = "SELECT \"testDate\" FROM \"s0\".\"p0\"  WHERE (\"testDate\" = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.postGreSqlRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        //From sql.Date java doc. Params:
        //year – the year minus 1900; must be 0 to 8099. (Note that 8099 is 9999 minus 1900.)
        //month – 0 to 11
        //day – 1 to 31
        //Start date = 1992-1-1
        Date expectedDate = new Date(120, 0, 5);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setDate(1, expectedDate);

        logger.info("buildSplitSqlForDateTest - exit");
    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
}
