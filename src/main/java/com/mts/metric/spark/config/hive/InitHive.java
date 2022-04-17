package com.mts.metric.spark.config.hive;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import static com.mts.metric.spark.config.Profiles.LOCAL;
import static com.mts.metric.spark.util.ResourceUtil.getResources;
import static com.mts.metric.spark.util.ResourceUtil.readContent;
import static org.springframework.core.Ordered.HIGHEST_PRECEDENCE;

@Component
@Profile(LOCAL)
public class InitHive {
    private final SparkSession session;
    private final ApplicationContext applicationContext;

    public InitHive(SparkSession session, ApplicationContext applicationContext) {
        this.session = session;
        this.applicationContext = applicationContext;
    }

    @EventListener
    @Order(HIGHEST_PRECEDENCE)
    public void onApplicationStart(ContextRefreshedEvent event) {
        createTables(applicationContext, session);
    }

    public static void createTables(ApplicationContext applicationContext, SparkSession session) {
        executeSQLScripts(
            getResources(applicationContext, "classpath:hive/ddl/create/*.hql"),
            session
        );
    }

    public static void dropTables(ApplicationContext applicationContext, SparkSession session) {
        executeSQLScripts(
            getResources(applicationContext, "classpath:hive/ddl/drop/*.hql"),
            session
        );
    }

    private static void executeSQLScripts(Resource[] resources, SparkSession session) {
        for (Resource resource : resources) {
            session.sql(readContent(resource));
        }
    }
}
