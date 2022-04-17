package com.mts.metric.spark.testutils.suite;

import com.mts.metric.spark.service.EnricherServiceTestConfiguration;
import com.mts.metric.spark.testutils.annotation.SparkIntegrationTest;

import org.springframework.test.context.ContextConfiguration;

@SparkIntegrationTest
@ContextConfiguration(classes = EnricherServiceTestConfiguration.class)
public class SparkIntegrationSuite extends IntegrationSuite {
}
