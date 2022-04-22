package com.mts.metric.spark.testutils.facade;

import com.mts.metric.spark.config.hive.InitHive;
import com.mts.metric.spark.testutils.facade.table.HiveTable;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

import java.util.function.Function;

@TestComponent
public class TestHiveUtils {
    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private InitHive initHive;

    public void cleanHive() {
        initHive.dropTables();
        initHive.createTables();
    }

    public <T, E extends HiveTable<T>> E insertInto(Function<SparkSession, E> tableFunction) {
        return tableFunction.apply(sparkSession);
    }
}
