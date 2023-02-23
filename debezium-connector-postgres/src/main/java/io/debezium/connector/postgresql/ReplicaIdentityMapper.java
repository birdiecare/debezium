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
 * Class that records Replica Identity information for the {@link PostgresConnector}
 * @author Ben White, Miguel Sotomayor
 */
@Immutable
public class ReplicaIdentityMapper {

    public static class ReplicaIdentityOption {
        private final ServerInfo.ReplicaIdentity replicaIdentity;
        private final String indexName;

        public ReplicaIdentityOption(ServerInfo.ReplicaIdentity replicaIdentity) {
            this.replicaIdentity = replicaIdentity;
            this.indexName = null;
        }

        public ReplicaIdentityOption(ServerInfo.ReplicaIdentity replicaIdentity, String indexName) {
            this.replicaIdentity = replicaIdentity;
            this.indexName = indexName;
        }

        public ServerInfo.ReplicaIdentity getReplicaIdentity() {
            return this.replicaIdentity;
        }

        public String getIndexName() {
            return this.indexName;
        }

        @Override
        public boolean equals(Object replicaIdentityOption) {
            if (replicaIdentityOption instanceof ReplicaIdentityOption) {
                ReplicaIdentityOption replicaIdentityOptionObj = (ReplicaIdentityOption) replicaIdentityOption;
                return replicaIdentityOptionObj.getReplicaIdentity().equals(this.getReplicaIdentity())
                        && (replicaIdentityOptionObj.getIndexName() != null && replicaIdentityOptionObj.getIndexName().equals(this.indexName) || (!this.replicaIdentity.equals(ServerInfo.ReplicaIdentity.INDEX)));
            }
            return false;
        }

        @Override
        public String toString() {
            return this.replicaIdentity == ServerInfo.ReplicaIdentity.INDEX ?
                    String.format("%s %s", this.replicaIdentity, this.indexName) :
                    this.replicaIdentity.toString();
        }

        @Override
        public int hashCode() {
            int result = 17;
            if (this.replicaIdentity != null) {
                result = 31 * result + this.replicaIdentity.hashCode();
            }
            if (this.indexName != null) {
                result = 31 * result + this.indexName.hashCode();
            }
            return result;
        }

    }

    public static final Pattern REPLICA_AUTO_SET_PATTERN = Pattern.compile("(?i)^\\s*([^\\s:]+):(DEFAULT|(INDEX) (.\\w*)|FULL|NOTHING)\\s*$");
    public static final Pattern PATTERN_SPLIT = Pattern.compile(";");

    private final Map<TableId, ReplicaIdentityOption> replicaIdentityMapper;

    public ReplicaIdentityMapper(String databaseName, String replicaAutoSetValue) {
        this.replicaIdentityMapper = getReplicaIdentityMapper(databaseName, replicaAutoSetValue);
    }

    private Map<TableId, ReplicaIdentityOption> getReplicaIdentityMapper(String databaseName, String replicaAutoSetValue) {

        if (replicaAutoSetValue == null) {
            return null;
        }

        return Arrays.stream(PATTERN_SPLIT.split(replicaAutoSetValue))
                .map(REPLICA_AUTO_SET_PATTERN::matcher)
                .filter(Matcher::matches)
                .collect(Collectors.toMap(
                        t -> {
                            String[] tableName = t.group(1).split("\\.");
                            return new TableId(databaseName, tableName[0], tableName[1]);
                        },
                        // Group 3 will be null if we are not processing `USING INDEX` as a replica identity
                        t -> t.group(3) != null ? new ReplicaIdentityOption(ServerInfo.ReplicaIdentity.valueOf(t.group(3)), t.group(4))
                                : new ReplicaIdentityOption(ServerInfo.ReplicaIdentity.valueOf(t.group(2)))
                ));
    }

    public Map<TableId, ReplicaIdentityOption> getMapper() {
        return this.replicaIdentityMapper;
    }

}
