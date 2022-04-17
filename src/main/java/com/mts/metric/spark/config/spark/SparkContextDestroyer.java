package com.mts.metric.spark.config.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class SparkContextDestroyer {
    private final JavaSparkContext sparkContext;

    public SparkContextDestroyer(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @PreDestroy
    void destroySparkContext() {
        sparkContext.close();
    }
}
