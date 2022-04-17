package com.mts.metric.spark.service;

import com.mts.metric.spark.testutils.suite.SparkIntegrationSuite;

class SexAgeHiveEnricherServiceUnitTest extends SparkIntegrationSuite {
    /*@Autowired
    private TestHiveUtils hive;
    @Autowired
    private TestAerospikeFacade aerospike;
    @Autowired
    private EnricherService sexAgeEnricherService;
    @Autowired
    private EnrichmentQualityService<SexAgeHiveEnrichmentResult> sexAgeQualityService;

    @BeforeEach
    void beforeEach() {
        aerospike.deleteAll();
        hive.cleanHive();
    }

    @Test
    void shouldSaveSingleRecordByMsisdn() throws Exception {
        hive.insertInto(vSexNAgeResults())
            .values(new VSexNAgeResults.Values()
                .setAge("12_32")
                .setMsisdn("123")
                .setSex(FEMALE));

        sexAgeEnricherService.proceedEnrichment().get();

        List<KeyRecord> keyRecords =
            await()
                .atMost(TEN_SECONDS)
                .until(aerospike::scanMsisdnSet, hasSize(1));
        assertThat(
            keyRecords,
            hasRecord(
                "123",
                new JSONObject()
                    .put("sex", "F")
                    .put("age", "12_32")
            )
        );
        verify(sexAgeQualityService, times(1))
            .checkEnrichmentQuality(
                eq(aSexAgeHiveEnrichmentResult().sparkRowsCount(1).build())
            );
    }

    @Test
    void shouldSaveRecordWithTheLatestBusinessDt() throws Exception {
        LocalDateTime businessDt = LocalDateTime.of(
            LocalDate.of(1943, 11, 23),
            MIDNIGHT
        );
        String msisdn = "451";
        hive.insertInto(vSexNAgeResults())
            .values(
                new VSexNAgeResults.Values()
                    .setAge("34_56")
                    .setMsisdn(msisdn)
                    .setSex(MALE)
                    .setBusinessDt(businessDt),
                new VSexNAgeResults.Values()
                    .setAge("45_67")
                    .setMsisdn(msisdn)
                    .setSex(MALE)
                    .setBusinessDt(businessDt.plusDays(1))
            );

        sexAgeEnricherService.proceedEnrichment().get();

        List<KeyRecord> keyRecords =
            await()
                .atMost(TEN_SECONDS)
                .until(aerospike::scanMsisdnSet, hasSize(1));
        assertThat(
            keyRecords,
            hasRecord(
                msisdn,
                new JSONObject()
                    .put("sex", "M")
                    .put("age", "45_67")
            )
        );

        verify(sexAgeQualityService, times(1))
            .checkEnrichmentQuality(
                eq(aSexAgeHiveEnrichmentResult().sparkRowsCount(1).build())
            );
    }

    @Test
    void shouldSaveMultipleRecords() throws Exception {
        LocalDateTime businessDt = LocalDateTime.of(
            LocalDate.of(1943, 11, 23),
            MIDNIGHT
        );
        String msisdn1 = "452";
        String msisdn2 = "162414";
        hive.insertInto(vSexNAgeResults())
            .values(
                new VSexNAgeResults.Values()
                    .setAge("34_56")
                    .setMsisdn(msisdn1)
                    .setSex(MALE)
                    .setBusinessDt(businessDt),
                new VSexNAgeResults.Values()
                    .setAge("34_57")
                    .setMsisdn(msisdn1)
                    .setSex(FEMALE)
                    .setBusinessDt(businessDt.plusDays(1)),
                new VSexNAgeResults.Values()
                    .setAge("12_13")
                    .setMsisdn(msisdn2)
                    .setSex(MALE)
                    .setBusinessDt(businessDt),
                new VSexNAgeResults.Values()
                    .setAge("56_67")
                    .setMsisdn(msisdn2)
                    .setSex(MALE)
                    .setBusinessDt(businessDt.plusDays(1))
            );

        sexAgeEnricherService.proceedEnrichment().get();

        List<KeyRecord> keyRecords =
            await()
                .atMost(TEN_SECONDS)
                .until(aerospike::scanMsisdnSet, hasSize(2));
        assertThat(
            keyRecords,
            allOf(
                hasRecord(
                    msisdn1,
                    new JSONObject()
                        .put("sex", "F")
                        .put("age", "34_57")
                ),
                hasRecord(
                    msisdn2,
                    new JSONObject()
                        .put("sex", "M")
                        .put("age", "56_67")
                )
            )
        );

        verify(sexAgeQualityService, times(1))
            .checkEnrichmentQuality(
                eq(aSexAgeHiveEnrichmentResult().sparkRowsCount(2).build())
            );
    }

    private static Matcher<List<KeyRecord>> hasRecord(String expectedKey, JSONObject expectedJsonBin) {
        return new TypeSafeMatcher<List<KeyRecord>>() {
            @Override
            protected boolean matchesSafely(List<KeyRecord> item) {
                return item.stream()
                    .anyMatch(keyRecord -> {
                        try {
                            boolean keysEqual = Objects.equals(keyRecord.key.userKey.getObject(), expectedKey);
                            JSONAssert.assertEquals(expectedJsonBin, new JSONObject(keyRecord.record.getString("json-repr")), false);
                            return keysEqual;
                        } catch (JSONException | AssertionError e) {
                            return false;
                        }
                    });
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                    format("[Key=%s, JSONBin=%s]", expectedKey, expectedJsonBin)
                );
            }

            @Override
            protected void describeMismatchSafely(List<KeyRecord> item, Description mismatchDescription) {
                mismatchDescription.appendText(item.toString());
            }
        };
    }*/
}