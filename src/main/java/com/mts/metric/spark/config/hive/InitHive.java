package com.mts.metric.spark.config.hive;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import static com.mts.metric.spark.config.Profiles.LOCAL;
import static com.mts.metric.spark.util.ResourceUtil.getResources;
import static com.mts.metric.spark.util.ResourceUtil.readContent;

@Component
@Profile(LOCAL)
public class InitHive {
    private final SparkSession session;
    private final ApplicationContext applicationContext;

    public InitHive(SparkSession session, ApplicationContext applicationContext) {
        this.session = session;
        this.applicationContext = applicationContext;
    }

    public void createTables() {
        executeSQLScripts(getResources(applicationContext, "classpath:hive/ddl/create/*.hql"));
    }

    public void dropTables() {
        executeSQLScripts(getResources(applicationContext, "classpath:hive/ddl/drop/*.hql"));
    }

    private void executeSQLScripts(Resource[] resources) {
        for (Resource resource : resources) {
            session.sql(readContent(resource));
        }
    }
}
