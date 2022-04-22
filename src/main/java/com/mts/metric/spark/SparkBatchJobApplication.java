package com.mts.metric.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class SparkBatchJobApplication {
    public static void main(String[] args) {
        SpringApplication.run(SparkBatchJobApplication.class, args);
    }
}
