/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;

public class ReplicaIdentityOptionMapperTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        String databaseName = "database_test";

        Map<TableId, ReplicaIdentityMapper.ReplicaIdentityOption> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), new ReplicaIdentityMapper.ReplicaIdentityOption(ServerInfo.ReplicaIdentity.FULL));
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"), new ReplicaIdentityMapper.ReplicaIdentityOption(ServerInfo.ReplicaIdentity.DEFAULT));

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:DEFAULT";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(databaseName, replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getMapper());
    }

    @Test
    public void shouldSetReplicaAutoSetIndexValue() {

        String databaseName = "database_test";

        Map<TableId, ReplicaIdentityMapper.ReplicaIdentityOption> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), new ReplicaIdentityMapper.ReplicaIdentityOption(ServerInfo.ReplicaIdentity.FULL));
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"), new ReplicaIdentityMapper.ReplicaIdentityOption(ServerInfo.ReplicaIdentity.INDEX, "idx_pk"));

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:INDEX idx_pk";

        ReplicaIdentityMapper replicaIdentityMapper = new ReplicaIdentityMapper(databaseName, replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getMapper());
    }
}
