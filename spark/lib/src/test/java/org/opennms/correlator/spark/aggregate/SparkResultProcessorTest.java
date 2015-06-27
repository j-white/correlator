package org.opennms.correlator.spark.aggregate;

import static org.opennms.correlator.spark.Utils.assertRDDRowsEqual;
import static org.opennms.newts.api.query.StandardAggregationFunctions.AVERAGE;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opennms.correlator.spark.Utils.SampleRDDRowsBuilder;
import org.opennms.correlator.spark.Utils.MeasurementRDDRowsBuilder;
import org.opennms.correlator.spark.aggregate.SparkResultProcessor;
import org.opennms.correlator.spark.aggregate.Sum;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;

@Ignore
public class SparkResultProcessorTest {

    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        context = new JavaSparkContext(conf);
    }

    @Test
    public void testCalculated() {

        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.COUNTER)
                .row(900000000).element("m0",  3000).element("m1",  3000)      // Thu Jul  9 11:00:00 CDT 1998
                .row(900000300).element("m0",  6000).element("m1",  6000)
                .row(900000600).element("m0",  9000).element("m1",  9000)
                .row(900000900).element("m0", 12000).element("m1", 12000)
                .row(900001200).element("m0", 15000).element("m1", 15000)
                .row(900001500).element("m0", 18000).element("m1", 18000)
                .row(900001800).element("m0", 21000).element("m1", 21000)
                .row(900002100).element("m0", 24000).element("m1", 24000)
                .row(900002400).element("m0", 27000).element("m1", 27000)
                .row(900002700).element("m0", 30000).element("m1", 30000)
                .row(900003000).element("m0", 33000).element("m1", 33000)
                .row(900003300).element("m0", 36000).element("m1", 36000)
                .row(900003600).element("m0", 39000).element("m1", 39000)
                .row(900003900).element("m0", 42000).element("m1", 42000)
                .row(900004200).element("m0", 45000).element("m1", 45000)
                .row(900004500).element("m0", 48000).element("m1", 48000)
                .row(900004800).element("m0", 51000).element("m1", 51000)
                .row(900005100).element("m0", 54000).element("m1", 54000)
                .row(900005400).element("m0", 57000).element("m1", 57000)
                .row(900005700).element("m0", 60000).element("m1", 60000)
                .row(900006000).element("m0", 63000).element("m1", 63000)
                .row(900006300).element("m0", 66000).element("m1", 66000)
                .row(900006600).element("m0", 69000).element("m1", 69000)
                .row(900006900).element("m0", 72000).element("m1", 72000)
                .row(900007200).element("m0", 75000).element("m1", 75000)      // Thu Jul  9 13:00:00 CDT 1998
                .build();

        ResultDescriptor rDescriptor = new ResultDescriptor(Duration.seconds(300))
                .datasource("m0", AVERAGE)
                .datasource("m1", AVERAGE)
                .calculate("total", new Sum(), "m0", "m1")
                .export("total");

        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(900003600).element("total", 20)
                .row(900007200).element("total", 20)
                .build();

        SparkResultProcessor processor = new SparkResultProcessor(
                context,
                new Resource("localhost"),
                Timestamp.fromEpochSeconds(900003600),
                Timestamp.fromEpochSeconds(900007200),
                rDescriptor,
                Duration.minutes(60));

        assertRDDRowsEqual(expected, processor.process(testData));

    }

    @Test
    public void testCounterRate() {

        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.COUNTER)
                .row(900000000).element("m0",  3000)        // Thu Jul  9 11:00:00 CDT 1998
                .row(900000300).element("m0",  6000)
                .row(900000600).element("m0",  9000)
                .row(900000900).element("m0", 12000)
                .row(900001200).element("m0", 15000)
                .row(900001500).element("m0", 18000)
                .row(900001800).element("m0", 21000)
                .row(900002100).element("m0", 24000)
                .row(900002400).element("m0", 27000)
                .row(900002700).element("m0", 30000)
                .row(900003000).element("m0", 33000)
                .row(900003300).element("m0", 36000)
                .row(900003600).element("m0", 39000)
                .row(900003900).element("m0", 42000)
                .row(900004200).element("m0", 45000)
                .row(900004500).element("m0", 48000)
                .row(900004800).element("m0", 51000)
                .row(900005100).element("m0", 54000)
                .row(900005400).element("m0", 57000)
                .row(900005700).element("m0", 60000)
                .row(900006000).element("m0", 63000)
                .row(900006300).element("m0", 66000)
                .row(900006600).element("m0", 69000)
                .row(900006900).element("m0", 72000)
                .row(900007200).element("m0", 75000)        // Thu Jul  9 13:00:00 CDT 1998
                .build();

        ResultDescriptor rDescriptor = new ResultDescriptor(Duration.seconds(300)).datasource("m0", AVERAGE).export("m0");

        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(900003600).element("m0", 10.0)
                .row(900007200).element("m0", 10.0)
                .build();

        SparkResultProcessor processor = new SparkResultProcessor(
                context,
                new Resource("localhost"),
                Timestamp.fromEpochSeconds(900003600),
                Timestamp.fromEpochSeconds(900007200),
                rDescriptor,
                Duration.minutes(60));

        assertRDDRowsEqual(expected, processor.process(testData));

    }

    @Test
    public void test() {

        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(900000000).element("m0", 1)        // Thu Jul  9 11:00:00 CDT 1998
                .row(900000300).element("m0", 1)
                .row(900000600).element("m0", 1)
                .row(900000900).element("m0", 1)
                .row(900001200).element("m0", 1)
                .row(900001500).element("m0", 1)
                .row(900001800).element("m0", 1)
                .row(900002100).element("m0", 3)
                .row(900002400).element("m0", 3)
                .row(900002700).element("m0", 3)
                .row(900003000).element("m0", 3)
                .row(900003300).element("m0", 3)
                .row(900003600).element("m0", 3)
                .row(900003900).element("m0", 1)
                .row(900004200).element("m0", 1)
                .row(900004500).element("m0", 1)
                .row(900004800).element("m0", 1)
                .row(900005100).element("m0", 1)
                .row(900005400).element("m0", 1)
                .row(900005700).element("m0", 3)
                .row(900006000).element("m0", 3)
                .row(900006300).element("m0", 3)
                .row(900006600).element("m0", 3)
                .row(900006900).element("m0", 3)
                .row(900007200).element("m0", 3)        // Thu Jul  9 13:00:00 CDT 1998
                .build();

        ResultDescriptor rDescriptor = new ResultDescriptor(Duration.seconds(300))
                .datasource("m0-avg", "m0", Duration.seconds(600), AVERAGE).export("m0-avg");

        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(900003600).element("m0-avg", 2.0)
                .row(900007200).element("m0-avg", 2.0)
                .build();

        SparkResultProcessor processor = new SparkResultProcessor(
                context,
                new Resource("localhost"),
                Timestamp.fromEpochSeconds(900003600),
                Timestamp.fromEpochSeconds(900007200),
                rDescriptor,
                Duration.minutes(60));

        assertRDDRowsEqual(expected, processor.process(testData));

    }
}
