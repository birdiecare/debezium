/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.annotation.Immutable;
import io.debezium.config.EnumeratedValue;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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


    public static Map<String, ReplicaIdentityMode> getReplicaIdentityMode(String replicaAutoSetTypes) {

        return Arrays.stream(PATTERN_SPLIT.split(replicaAutoSetTypes))
                .map(REPLICA_AUTO_SET_PATTERN::matcher)
                .filter(Matcher::find)
                .collect(Collectors.toMap(
                        t -> t.group(1),
                        t -> ReplicaIdentityMode.valueOf(t.group(2))
                ));
    }

}
