package com.mts.metric.spark;

import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class SparkBatchJobApplication {

    public static void main(String[] args) {
        // BasicConfigurator.configure();
        SpringApplication.run(SparkBatchJobApplication.class, args);
    }

}
