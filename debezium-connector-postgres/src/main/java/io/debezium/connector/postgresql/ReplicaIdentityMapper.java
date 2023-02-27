/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.annotation.Immutable;
import io.debezium.connector.postgresql.connection.ServerInfo;
import io.debezium.relational.TableId;

/**
 *
 * @author Ben White, Miguel Sotomayor
 */
@Immutable
public class ReplicaIdentityMapper {

    public static final Pattern REPLICA_AUTO_SET_PATTERN = Pattern.compile("(?i)^\\s*([^\\s:]+):(DEFAULT|USING INDEX|FULL|NOTHING)\\s*$");
    public static final Pattern PATTERN_SPLIT = Pattern.compile(";");

    private final Map<TableId, ServerInfo.ReplicaIdentity> replicaIdentityMapper;

    public ReplicaIdentityMapper(String databaseName, String replicaAutoSetValue) {
        this.replicaIdentityMapper = getReplicaIdentityMapper(databaseName, replicaAutoSetValue);
    }

    private static Map<TableId, ServerInfo.ReplicaIdentity> getReplicaIdentityMapper(String databaseName, String replicaAutoSetValue) {

        if (replicaAutoSetValue == null) {
            return null;
        }

        return Arrays.stream(PATTERN_SPLIT.split(replicaAutoSetValue))
                .map(REPLICA_AUTO_SET_PATTERN::matcher)
                .filter(Matcher::find)
                .collect(Collectors.toMap(
                        t -> {
                            String[] tableName = t.group(1).split("\\.");
                            return new TableId(databaseName, tableName[0], tableName[1]);
                        },
                        t -> ServerInfo.ReplicaIdentity.valueOf(t.group(2))));
    }

    public Map<TableId, ServerInfo.ReplicaIdentity> getMapper() {
        return this.replicaIdentityMapper;
    }

}
