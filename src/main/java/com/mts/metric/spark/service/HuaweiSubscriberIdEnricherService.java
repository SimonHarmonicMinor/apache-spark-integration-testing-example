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

@Service
public class HuaweiSubscriberIdEnricherService implements EnricherService, Serializable {
    private static final long serialVersionUID = 10L;
    private static final Logger LOG = LoggerFactory.getLogger(HuaweiSubscriberIdEnricherService.class);

    private final SparkSession session;
    private final AerospikeProperties aerospikeProperties;

    public HuaweiSubscriberIdEnricherService(SparkSession session,
                                             AerospikeProperties aerospikeProperties) {
        this.session = session;
        this.aerospikeProperties = aerospikeProperties;
    }

    @Override
    public void proceedEnrichment() {
        Dataset<Row> dataset = session.sql(
            "SELECT subscriber_id, msisdn FROM mediatv_dds.subscriber_info " +
                "WHERE msisdn IS NOT NULL AND subscriber_id IS NOT NULL"
        );
        dataset
            .foreachPartition(
                iterator -> {
                    final var aerospikeClient = newAerospikeClient(aerospikeProperties);
                    iterator.forEachRemaining(row -> {
                        String huaweiSubscriberId = row.getString(row.fieldIndex("subscriber_id"));
                        String msisdn = row.getString(row.fieldIndex("msisdn"));
                        Key key = new Key("my-namespace", "huawei", huaweiSubscriberId);
                        Bin bin = new Bin("msisdn", msisdn);
                        try {
                            aerospikeClient.put(null, key, bin);
                            LOG.info("Record has been successfully added {}", key);
                        } catch (Exception e) {
                            LOG.error("Fail during inserting record to Aerospike", e);
                        }
                    });
                }
            );
    }
}
