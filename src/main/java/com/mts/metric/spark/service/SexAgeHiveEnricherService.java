package com.mts.metric.spark.service;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.mts.metric.spark.properties.aerospike.AerospikeProperties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static com.mts.metric.spark.config.aerospike.AerospikeFactory.newAerospikeClient;
import static java.lang.String.format;

@Service
class SexAgeHiveEnricherService implements EnricherService, Serializable {
    private static final long serialVersionUID = 13L;
    private static final Logger LOG = LoggerFactory.getLogger(SexAgeHiveEnricherService.class);

    private final SparkSession session;
    private final AerospikeProperties aerospikeProperties;

    public SexAgeHiveEnricherService(SparkSession session,
                                     AerospikeProperties aerospikeProperties) {
        this.session = session;
        this.aerospikeProperties = aerospikeProperties;
    }

    @Override
    public void proceedEnrichment() {
        Dataset<Row> dataset =
            session.sql(
                "SELECT msisdn, age, sex\n" +
                    "FROM (SELECT msisdn,\n" +
                    "             age,\n" +
                    "             sex,\n" +
                    "             ROW_NUMBER()\n" +
                    "             OVER (\n" +
                    "                 PARTITION BY msisdn\n" +
                    "                 ORDER BY from_unixtime(unix_timestamp(business_dt, 'yyyy-MM-dd')) DESC\n" +
                    "                 ) AS rn\n" +
                    "      FROM ff_dm.v_sex_n_age_results\n" +
                    "     ) sub\n" +
                    "WHERE sub.rn = 1"
            );
        dataset
            .foreachPartition(
                iterator -> {
                    final var aerospikeClient = newAerospikeClient(aerospikeProperties);
                    iterator.forEachRemaining(row -> {
                        Key key = new Key("my-namespace", "msisdn", row.<String>getAs("msisdn"));
                        Bin bin = new Bin("json", "{}");
                        try {
                            aerospikeClient.put(null, key, bin);
                            LOG.info("Record has been successfully added to Aerospike: {}", key);
                        } catch (Exception e) {
                            LOG.error(
                                format("Fail during insert record to Aerospike: %s", row),
                                e
                            );
                        }
                    });
                }
            );
    }
}
