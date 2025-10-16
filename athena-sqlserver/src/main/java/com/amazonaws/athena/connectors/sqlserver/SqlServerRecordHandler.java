/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerRecordHandler extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerRecordHandler.class);
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    public SqlServerRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SqlServerConstants.NAME, configOptions), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new SqlServerJdbcConnectionFactory(databaseConnectionConfig, SqlServerMetadataHandler.JDBC_PROPERTIES,
                new DatabaseConnectionInfo(SqlServerConstants.DRIVER_CLASS, SqlServerConstants.DEFAULT_PORT)), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(),
                AthenaClient.create(), jdbcConnectionFactory, new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final S3Client amazonS3, final SecretsManagerClient secretsManager,
                           final AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints,
                                           Split split) throws SQLException
    {
        LOGGER.info("=== SQLServer RECORD HANDLER ===");
        LOGGER.info("Building SQL for table: {}.{}", tableName.getSchemaName(), tableName.getTableName());
        LOGGER.info("Query passThrough: {}", constraints.isQueryPassThrough());

        long startTime = System.currentTimeMillis();
        PreparedStatement preparedStatement;

        if (constraints.isQueryPassThrough()) {
            LOGGER.info("Using Query passThrough approach");
            preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
        }
        else {
            LOGGER.info("Using SQLServer JDBC split query builder");
            preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(),
                    schema, constraints, split);
        }
        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);
        long buildTime = System.currentTimeMillis() - startTime;
        LOGGER.info("SQLServer-  SQL build completed in {} ms with fetch size: {}", buildTime, FETCH_SIZE);
        LOGGER.info("Prepare Statement ----- "+preparedStatement);
        return preparedStatement;
    }
}
