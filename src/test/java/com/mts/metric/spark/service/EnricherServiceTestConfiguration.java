package com.mts.metric.spark.service;

import com.mts.metric.spark.properties.aerospike.AerospikeProperties;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class EnricherServiceTestConfiguration {

    @Bean
    public EnricherService subscriberEnricherService(SparkSession session,
                                                     AerospikeProperties aerospikeProperties) {
        return new SubscriberIdEnricherService(
            session, aerospikeProperties
        );
    }
}
