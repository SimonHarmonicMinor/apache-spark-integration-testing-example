package com.mts.metric.spark.testutils.facade.table;

import org.apache.spark.sql.SparkSession;

import java.util.function.Function;

import static java.lang.String.format;

public class SubscriberInfo implements HiveTable<SubscriberInfo.Values> {
    private static final String table = "mediatv_dds.subscriber_info";

    private final SparkSession session;

    public SubscriberInfo(SparkSession session) {
        this.session = session;
    }

    public static Function<SparkSession, SubscriberInfo> subscriberInfo() {
        return SubscriberInfo::new;
    }

    @Override
    public void values(Values... values) {
        for (Values value : values) {
            session.sql(
                format(
                    "insert into %s values(" +
                        "'%s', '%s', %d" +
                        ")",
                    table,
                    value.subscriberId,
                    value.msisdn,
                    value.activeFlag
                )
            );
        }
    }

    public static class Values {
        private String subscriberId = "4121521";
        private String msisdn = "88005553535";
        private int activeFlag = 0;

        public Values setSubscriberId(String subscriberId) {
            this.subscriberId = subscriberId;
            return this;
        }

        public Values setMsisdn(String msisdn) {
            this.msisdn = msisdn;
            return this;
        }

        public Values setActiveFlag(int activeFlag) {
            this.activeFlag = activeFlag;
            return this;
        }
    }
}
