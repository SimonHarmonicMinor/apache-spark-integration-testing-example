package com.mts.metric.spark.config.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.mts.metric.spark.properties.aerospike.AerospikeProperties;

import java.util.List;

import static java.util.stream.Collectors.toList;

public final class AerospikeFactory {
    private AerospikeFactory() {
        // no op
    }

    public static AerospikeClient newAerospikeClient(AerospikeProperties properties) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.timeout = 10_000;
        clientPolicy.writePolicyDefault.sendKey = true;
        clientPolicy.eventLoops = new NioEventLoops();
        return new AerospikeClient(
            clientPolicy,
            getHosts(properties).toArray(new Host[]{})
        );
    }

    public static List<Host> getHosts(AerospikeProperties properties) {
        return properties
            .getHosts()
            .stream()
            .map(host -> new Host(host.getName(), host.getPort()))
            .collect(toList());
    }
}
