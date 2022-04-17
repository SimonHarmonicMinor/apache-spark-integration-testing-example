package com.mts.metric.spark.testutils.facade.table;

import org.apache.spark.sql.SparkSession;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.function.Function;

import static com.mts.metric.spark.testutils.facade.table.VSexNAgeResults.Values.Sex.MALE;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;

public class VSexNAgeResults implements HiveTable<VSexNAgeResults.Values> {
    private static final String table = "ff_dm.v_sex_n_age_results";

    private final SparkSession session;

    public VSexNAgeResults(SparkSession session) {
        this.session = session;
    }

    public static Function<SparkSession, VSexNAgeResults> vSexNAgeResults() {
        return VSexNAgeResults::new;
    }

    @Override
    public void values(Values... values) {
        for (Values value : values) {
            session.sql(
                format(
                    "insert into %s values(" +
                        "'%s', %s, %d, '%s', '%s', '%s', '%s'" +
                        ")",
                    table,
                    value.msisdn,
                    value.appN.toString(),
                    value.regid,
                    value.sex == MALE ? "M" : "F",
                    value.age,
                    value.source,
                    value.businessDt.format(ISO_DATE)
                )
            );
        }
    }

    public static class Values {
        private String msisdn = "1004800284";
        private BigInteger appN = new BigInteger("249708602");
        private int regid = 50;
        private Sex sex = MALE;
        private String age = "55_64";
        private String source = "mdm";
        private LocalDateTime businessDt = LocalDateTime.now();

        public enum Sex {
            MALE, FEMALE
        }

        public Values setMsisdn(String msisdn) {
            this.msisdn = msisdn;
            return this;
        }

        public Values setAppN(BigInteger appN) {
            this.appN = appN;
            return this;
        }

        public Values setRegid(int regid) {
            this.regid = regid;
            return this;
        }

        public Values setSex(Sex sex) {
            this.sex = sex;
            return this;
        }

        public Values setAge(String age) {
            this.age = age;
            return this;
        }

        public Values setSource(String source) {
            this.source = source;
            return this;
        }

        public Values setBusinessDt(LocalDateTime businessDt) {
            this.businessDt = businessDt;
            return this;
        }
    }
}
