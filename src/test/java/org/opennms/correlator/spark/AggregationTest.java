package org.opennms.correlator.spark;

import static java.lang.Double.NaN;
import static org.opennms.newts.api.query.StandardAggregationFunctions.AVERAGE;
import static org.opennms.newts.api.query.StandardAggregationFunctions.MAX;
import static org.opennms.newts.api.query.StandardAggregationFunctions.MIN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.correlator.spark.Utils.MeasurementRDDRowsBuilder;
import org.opennms.correlator.spark.aggregate.Aggregation;

import static org.opennms.correlator.spark.Utils.assertRDDRowsEqual;

public class AggregationTest {

    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {
        String sparkMasterUrl = "local";
        SparkConf conf = new SparkConf().setAppName("test")
                .setMaster(sparkMasterUrl);
        context = new JavaSparkContext(conf);
    }

    @Test
    public void test() {
        JavaRDD<Row<Measurement>> testData = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(   1).element("m0", 1)
                .row( 300).element("m0", 1)
                .row( 600).element("m0", 1)
                .row( 900).element("m0", 1)
                .row(1200).element("m0", 1)
                .row(1500).element("m0", 1)
                .row(1800).element("m0", 3)
                .row(2100).element("m0", 3)
                .row(2400).element("m0", 3)
                .row(2700).element("m0", 3)
                .row(3000).element("m0", 3)
                .row(3300).element("m0", 3)
                .build();

        ResultDescriptor rDescriptor = new ResultDescriptor(Duration.seconds(300))
                .datasource("m0-avg", "m0", Duration.seconds(600), AVERAGE)
                .datasource("m0-min", "m0", Duration.seconds(600), MIN)
                .datasource("m0-max", "m0", Duration.seconds(600), MAX);

        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(   0).element("m0-avg", NaN).element("m0-min", NaN).element("m0-max", NaN)
                .row(3600).element("m0-avg",   2).element("m0-min",   1).element("m0-max",   3)
                .build();

        JavaRDD<Row<Measurement>> actual = Aggregation.aggregate(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(1),
                Timestamp.fromEpochSeconds(3300),
                rDescriptor,
                Duration.minutes(60));

        assertRDDRowsEqual(expected, actual);

    }
}
