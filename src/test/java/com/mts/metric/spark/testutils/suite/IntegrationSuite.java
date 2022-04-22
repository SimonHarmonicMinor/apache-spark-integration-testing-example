package com.mts.metric.spark.testutils.suite;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ContextConfiguration(initializers = IntegrationSuite.Initializer.class)
public class IntegrationSuite {
    private static final String AEROSPIKE_IMAGE = "aerospike/aerospike-server:5.6.0.4";

    static class Initializer implements
        ApplicationContextInitializer<ConfigurableApplicationContext> {

        static final GenericContainer<?> aerospike =
            new GenericContainer<>(DockerImageName.parse(AEROSPIKE_IMAGE))
                .withExposedPorts(3000, 3001, 3002)
                .withEnv("NAMESPACE", "my-namespace")
                .withEnv("SERVICE_PORT", "3000")
                .waitingFor(Wait.forLogMessage(".*migrations: complete.*", 1));

        @Override
        public void initialize(
            ConfigurableApplicationContext applicationContext) {

            startContainers();
            aerospike.followOutput(
                new Slf4jLogConsumer(LoggerFactory.getLogger("Aerospike"))
            );
            ConfigurableEnvironment environment =
                applicationContext.getEnvironment();

            MapPropertySource testcontainers = new MapPropertySource(
                "testcontainers",
                createConnectionConfiguration()
            );

            environment.getPropertySources().addFirst(testcontainers);
        }

        private static void startContainers() {
            Startables.deepStart(Stream.of(aerospike)).join();
        }

        private static Map<String, Object> createConnectionConfiguration() {
            return Map.of(
                "aerospike.hosts",
                Stream.of(3000, 3001, 3002)
                    .map(port -> aerospike.getHost() + ":" + aerospike.getMappedPort(port))
                    .collect(Collectors.joining(","))
            );
        }
    }
}

