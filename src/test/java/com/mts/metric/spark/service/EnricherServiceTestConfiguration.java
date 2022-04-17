package com.mts.metric.spark.service;

import com.mts.metric.spark.properties.aerospike.AerospikeProperties;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class EnricherServiceTestConfiguration {

    @Bean
    public EnricherService huaweiEnricherService(SparkSession session,
                                                 AerospikeProperties aerospikeProperties) {
        return new HuaweiSubscriberIdEnricherService(
            session, aerospikeProperties
        );
    }

    @Bean
    public EnricherService sexAgeEnricherService(SparkSession session,
                                                 AerospikeProperties aerospikeProperties) {
        return new SexAgeHiveEnricherService(
            session,
            aerospikeProperties
        );
    }
}
