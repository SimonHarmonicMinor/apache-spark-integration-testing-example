package com.mts.metric.spark.config.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.mts.metric.spark.config.Profiles.LOCAL;
import static com.mts.metric.spark.config.Profiles.PROD;

@Configuration
public class SparkConfig {
    @Value("${spring.application.name}")
    private String appName;

    @Bean
    @Profile(LOCAL)
    public Path localHivePath() throws IOException {
        return Files.createTempDirectory("hiveDataWarehouse");
    }

    @Bean
    @Profile(LOCAL)
    public SparkConf localSparkConf(Path localHivePath) throws IOException {
        FileSystemUtils.deleteRecursively(localHivePath);
        return new SparkConf()
            .setAppName(appName)
            .setMaster("local")
            .set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:local;create=true")
            .set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
            .set("hive.stats.jdbc.timeout", "80")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.session.timeZone", "UTC")
            .set("spark.sql.catalogImplementation", "hive")
            .set("spark.sql.warehouse.dir", localHivePath.toAbsolutePath().toString());
    }

    @Bean
    @Profile(PROD)
    public SparkConf prodSparkConf() {
        return new SparkConf()
            .setAppName(appName);
    }

    @Bean
    public JavaSparkContext javaSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

    @Bean
    @Profile({PROD, LOCAL})
    public SparkSession sparkSessionSupplier(JavaSparkContext sparkContext) {
        return SparkSession.builder()
            .sparkContext(sparkContext.sc())
            .config(sparkContext.getConf())
            .enableHiveSupport()
            .getOrCreate();
    }
}
