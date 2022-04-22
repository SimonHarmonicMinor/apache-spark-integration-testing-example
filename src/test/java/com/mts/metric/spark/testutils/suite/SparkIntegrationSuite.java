package com.mts.metric.spark.testutils.suite;

import com.mts.metric.spark.config.PropertiesConfig;
import com.mts.metric.spark.config.aerospike.AerospikeConfig;
import com.mts.metric.spark.config.hive.InitHive;
import com.mts.metric.spark.config.spark.SparkConfig;
import com.mts.metric.spark.config.spark.SparkContextDestroyer;
import com.mts.metric.spark.service.EnricherServiceTestConfiguration;
import com.mts.metric.spark.testutils.facade.TestAerospikeFacade;
import com.mts.metric.spark.testutils.facade.TestHiveUtils;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static com.mts.metric.spark.config.Profiles.LOCAL;

@SpringBootTest(
    classes = {
        SparkConfig.class,
        SparkContextDestroyer.class,
        AerospikeConfig.class,
        PropertiesConfig.class,
        InitHive.class,
        TestHiveUtils.class,
        TestAerospikeFacade.class,
        EnricherServiceTestConfiguration.class
    }
)
@ActiveProfiles(LOCAL)
public class SparkIntegrationSuite extends IntegrationSuite {
}
