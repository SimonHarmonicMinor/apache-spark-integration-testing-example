package com.mts.metric.spark.testutils.facade;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.query.KeyRecord;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@TestComponent
public class TestAerospikeFacade {
    @Autowired
    private AerospikeClient aerospikeClient;

    public List<KeyRecord> scanAll(String namespace) {
        final var keyRecords = new CopyOnWriteArrayList<KeyRecord>();
        aerospikeClient.scanAll(
            null,
            namespace,
            null,
            (key, record) -> keyRecords.add(new KeyRecord(key, record))
        );
        return keyRecords;
    }

    public void deleteAll(String namespace) {
        aerospikeClient.truncate(null, namespace, null, null);
    }

    public void put(Key key, Bin... bins) {
        aerospikeClient.put(null, key, bins);
    }
}
