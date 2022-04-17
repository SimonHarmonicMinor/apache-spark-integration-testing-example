package com.mts.metric.spark.testutils.annotation;

import com.mts.metric.spark.config.PropertiesConfig;
import com.mts.metric.spark.config.aerospike.AerospikeConfig;
import com.mts.metric.spark.config.hive.InitHive;
import com.mts.metric.spark.config.spark.SparkConfig;
import com.mts.metric.spark.config.spark.SparkContextDestroyer;
import com.mts.metric.spark.testutils.facade.TestAerospikeFacade;
import com.mts.metric.spark.testutils.facade.TestHiveUtils;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.mts.metric.spark.config.Profiles.LOCAL;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
@SpringBootTest(
    classes = {
        SparkConfig.class,
        SparkContextDestroyer.class,
        AerospikeConfig.class,
        PropertiesConfig.class,
        InitHive.class,
        TestHiveUtils.Config.class,
        TestAerospikeFacade.Config.class
    }
)
@ActiveProfiles({LOCAL, "test"})
public @interface SparkIntegrationTest {
}
