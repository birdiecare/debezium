/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import io.debezium.connector.postgresql.connection.ServerInfo;
import org.junit.Test;

import io.debezium.relational.TableId;

public class ReplicaIdentityTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        String databaseName = "database_test";

        Map<TableId, ServerInfo.ReplicaIdentity> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), ServerInfo.ReplicaIdentity.FULL);
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"), ServerInfo.ReplicaIdentity.DEFAULT);

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:DEFAULT";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(databaseName, replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getMapper());
    }
}
