/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.annotation.Immutable;
import io.debezium.config.EnumeratedValue;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Ben White, Miguel Sotomayor
 */
@Immutable
public class ReplicaIdentity {

    public static final Pattern REPLICA_AUTO_SET_PATTERN = Pattern.compile("(?i)^\\s*([^\\s:]+):(DEFAULT|USING INDEX|FULL|NOTHING)\\s*$");
    public static final Pattern PATTERN_SPLIT = Pattern.compile(";");

    public enum ReplicaIdentityMode implements EnumeratedValue {

        DEFAULT("DEFAULT"),
        USING_INDEX("USING INDEX"),
        FULL("FULL"),
        NOTHING("NOTHING");

        private final String value;

        ReplicaIdentityMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }
    }


    // TODO: Can we improve this method?
    public static Map<String, ReplicaIdentityMode> getReplicaIdentityMode(String replicaAutoSetTypes) {

        Map<String, ReplicaIdentityMode> tableToReplicaIdentity = new HashMap<>();

        for (String substring : PATTERN_SPLIT.split(replicaAutoSetTypes)) {
            Pattern pattern = Pattern.compile(String.valueOf(REPLICA_AUTO_SET_PATTERN));
            Matcher matcher = pattern.matcher(substring);
            if (matcher.find() && (matcher.groupCount() == 2))
            {
                tableToReplicaIdentity.put(matcher.group(1), ReplicaIdentityMode.valueOf(matcher.group(2)));
            }
        }

        return tableToReplicaIdentity;
    }

}
