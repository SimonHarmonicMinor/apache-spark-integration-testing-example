package com.mts.metric.spark.service;

import com.aerospike.client.query.KeyRecord;
import com.mts.metric.spark.testutils.facade.TestAerospikeFacade;
import com.mts.metric.spark.testutils.facade.TestHiveUtils;
import com.mts.metric.spark.testutils.facade.table.SubscriberInfo;
import com.mts.metric.spark.testutils.suite.SparkIntegrationSuite;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Objects;

import static com.mts.metric.spark.testutils.facade.table.SubscriberInfo.subscriberInfo;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasSize;

class SubscriberIdEnricherServiceIntegrationTest extends SparkIntegrationSuite {
    @Autowired
    private TestHiveUtils hive;
    @Autowired
    private TestAerospikeFacade aerospike;
    @Autowired
    private EnricherService subscriberEnricherService;

    @BeforeEach
    void beforeEach() {
        aerospike.deleteAll("my-namespace");
        hive.cleanHive();
    }

    @Test
    void shouldSaveRecords() {
        hive.insertInto(subscriberInfo())
            .values(
                new SubscriberInfo.Values()
                    .setMsisdn("msisdn1")
                    .setSubscriberId("subscriberId1"),
                new SubscriberInfo.Values()
                    .setMsisdn("msisdn2")
                    .setSubscriberId("subscriberId2")
            );

        subscriberEnricherService.proceedEnrichment();

        List<KeyRecord> keyRecords = await()
            .atMost(TEN_SECONDS)
            .until(() -> aerospike.scanAll("my-namespace"), hasSize(2));
        assertThat(keyRecords, allOf(
            hasRecord("subscriberId1", "msisdn1"),
            hasRecord("subscriberId2", "msisdn2")
        ));
    }

    private static Matcher<List<KeyRecord>> hasRecord(String huaweiSubscriberId, String msisdn) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(List<KeyRecord> keyRecords) {
                return keyRecords.stream()
                    .anyMatch(
                        keyRecord ->
                            Objects.equals(keyRecord.key.userKey.getObject(), huaweiSubscriberId)
                                && Objects.equals(keyRecord.record.getString("msisdn"), msisdn)
                    );
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                    format("[Huawei(key)=%s, MSISDN(bin)=%s]", huaweiSubscriberId, msisdn)
                );
            }

            @Override
            protected void describeMismatchSafely(List<KeyRecord> item, Description mismatchDescription) {
                mismatchDescription.appendText(item.toString());
            }
        };
    }
}