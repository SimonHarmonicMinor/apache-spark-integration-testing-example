package com.mts.metric.spark.config.aerospike;

import com.aerospike.client.AerospikeClient;
import com.mts.metric.spark.properties.aerospike.AerospikeProperties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AerospikeConfig {
    private final AerospikeProperties aerospikeProperties;

    public AerospikeConfig(AerospikeProperties aerospikeProperties) {
        this.aerospikeProperties = aerospikeProperties;
    }

    @Bean
    public AerospikeClient aerospikeClient() {
        return AerospikeFactory.newAerospikeClient(aerospikeProperties);
    }
}
