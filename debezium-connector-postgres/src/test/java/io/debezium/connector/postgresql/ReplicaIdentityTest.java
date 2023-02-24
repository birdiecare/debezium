package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.debezium.relational.TableId;

public class ReplicaIdentityTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        String databaseName = "database_test";

        Map<TableId, ReplicaIdentity.ReplicaIdentityMode> expectedMap = new HashMap<>();
        expectedMap.put(new TableId(databaseName, "testSchema_1", "testTable_1"), ReplicaIdentity.ReplicaIdentityMode.FULL);
        expectedMap.put(new TableId(databaseName, "testSchema_2", "testTable_2"), ReplicaIdentity.ReplicaIdentityMode.DEFAULT);

        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:DEFAULT";

        ReplicaIdentity replicaIdentityMapper = new ReplicaIdentity(databaseName, replica_autoset_type_field);

        assertEquals(expectedMap, replicaIdentityMapper.getMapper());
    }
}
