package io.debezium.connector.postgresql;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReplicaIdentityTest {

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Map<String, ReplicaIdentity.ReplicaIdentityMode> expectedMap = new HashMap<>();
        expectedMap.put("testSchema_1.testTable_1", ReplicaIdentity.ReplicaIdentityMode.FULL);
        expectedMap.put("testSchema_2.testTable_2", ReplicaIdentity.ReplicaIdentityMode.DEFAULT);


        String replica_autoset_type_field = "testSchema_1.testTable_1:FULL;testSchema_2.testTable_2:DEFAULT";

        Map<String, ReplicaIdentity.ReplicaIdentityMode> hashMap = ReplicaIdentity.getReplicaIdentityMode(replica_autoset_type_field);

        assertEquals(expectedMap, hashMap);
    }
}
