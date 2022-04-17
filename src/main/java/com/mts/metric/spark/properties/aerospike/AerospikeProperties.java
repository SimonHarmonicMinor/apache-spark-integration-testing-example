package com.mts.metric.spark.properties.aerospike;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

import static java.util.stream.Collectors.toList;

@ConfigurationProperties(prefix = "aerospike")
@ConstructorBinding
public class AerospikeProperties implements Serializable {
    private static final long serialVersionUID = 4L;

    @NotNull
    private final List<AerospikeHost> hosts;

    public AerospikeProperties(String hosts) {
        this.hosts = Stream.of(hosts.split(","))
            .filter(s -> !s.isEmpty())
            .map(s -> {
                final var hostWithPort = s.split(":");
                return new AerospikeHost(hostWithPort[0], Integer.parseInt(hostWithPort[1]));
            })
            .collect(toList());
    }

    public List<AerospikeHost> getHosts() {
        return hosts;
    }

    public static class AerospikeHost implements Serializable {
        private static final long serialVersionUID = 3L;

        @NotNull
        private final String name;
        private final int port;

        public AerospikeHost(String name, int port) {
            this.name = name;
            this.port = port;
        }

        public String getName() {
            return name;
        }

        public int getPort() {
            return port;
        }
    }
}
