/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.config.ConfigDefinitionMetadataTest;
import io.debezium.config.Configuration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PostgresConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public PostgresConnectorConfigDefTest() {
        super(new PostgresConnector());
    }

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.REPLICA_AUTOSET_TYPE, "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build()
                , PostgresConnectorConfig.REPLICA_AUTOSET_TYPE
                , (field,value,problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetInvalidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_AUTOSET_TYPE, "testSchema_1.testTable_1,FULL;testSchema_2.testTable_2,,DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build()
                , PostgresConnectorConfig.REPLICA_AUTOSET_TYPE
                , (field,value,problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }
}
