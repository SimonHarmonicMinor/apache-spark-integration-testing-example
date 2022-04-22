package com.mts.metric.spark.testutils.facade.table;

import org.apache.spark.sql.SparkSession;

import java.util.function.Function;

import static java.lang.String.format;

public class SubscriberInfo implements HiveTable<SubscriberInfo.Values> {
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
            session.sql(format(
                    "insert into %s values('%s', '%s')",
                    "media.subscriber_info",
                    value.subscriberId,
                    value.msisdn
                )
            );
        }
    }

    public static class Values {
        private String subscriberId = "4121521";
        private String msisdn = "88005553535";

        public Values setSubscriberId(String subscriberId) {
            this.subscriberId = subscriberId;
            return this;
        }

        public Values setMsisdn(String msisdn) {
            this.msisdn = msisdn;
            return this;
        }
    }
}
