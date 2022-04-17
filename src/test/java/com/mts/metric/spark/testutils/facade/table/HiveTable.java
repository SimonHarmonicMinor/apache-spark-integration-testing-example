package com.mts.metric.spark.testutils.facade.table;

public interface HiveTable<T> {
    /**
     * Insert values to Hive table.
     *
     * @param t values to insert
     */
    @SuppressWarnings("unchecked")
    void values(T... t);
}
