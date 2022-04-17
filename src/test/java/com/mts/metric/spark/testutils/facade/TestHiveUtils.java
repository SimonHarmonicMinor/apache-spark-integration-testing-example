package com.mts.metric.spark.testutils.facade;

import com.mts.metric.spark.testutils.facade.table.HiveTable;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

import static com.mts.metric.spark.config.hive.InitHive.createTables;
import static com.mts.metric.spark.config.hive.InitHive.dropTables;

public class TestHiveUtils {
    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private ApplicationContext applicationContext;

    public void cleanHive() {
        dropTables(applicationContext, sparkSession);
        createTables(applicationContext, sparkSession);
    }

    public <T, E extends HiveTable<T>> E insertInto(Function<SparkSession, E> tableFunction) {
        return tableFunction.apply(sparkSession);
    }

    @TestConfiguration
    public static class Config {
        @Bean
        public TestHiveUtils testHiveUtils() {
            return new TestHiveUtils();
        }
    }
}
