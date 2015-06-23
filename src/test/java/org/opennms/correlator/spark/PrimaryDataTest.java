package org.opennms.correlator.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.opennms.correlator.spark.Utils.assertRDDRowsEqual;
import static org.opennms.newts.api.query.StandardAggregationFunctions.AVERAGE;

import org.opennms.correlator.spark.Utils.SampleRDDRowsBuilder;
import org.opennms.correlator.spark.Utils.MeasurementRDDRowsBuilder;
import org.opennms.correlator.spark.aggregate.PrimaryData;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;

public class PrimaryDataTest {

    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {
        String sparkMasterUrl = "local";
        SparkConf conf = new SparkConf().setAppName("test")
                .setMaster(sparkMasterUrl);
        context = new JavaSparkContext(conf);
    }
    
    @Test
    public void testLeadingSamplesMiss() {

        // Missing a couple leading samples
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(900000600).element("m0", 1)
                .row(900000900).element("m0", 2)
                .row(900001200).element("m0", 3)
                .build();

        ResultDescriptor rDescriptor = new ResultDescriptor(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(900000000).element("m0", Double.NaN)
                .row(900000300).element("m0", Double.NaN)
                .row(900000600).element("m0", 1)
                .row(900000900).element("m0", 2)
                .row(900001200).element("m0", 3)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(900000000),
                Timestamp.fromEpochSeconds(900001200),
                rDescriptor);

        /*
        for (Row<Measurement> row : actual.collect()) {
            System.err.println(String.format("%d: %s", row.getTimestamp().asSeconds(), row));
        }
        */
        
        assertRDDRowsEqual(expected, actual);
    }
    
    @Test
    public void testShortSamples() {

        // Samples occur prior to the nearest step interval boundary.
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(000).element("m0", 0).element("m1", 1)
                .row(250).element("m0", 1).element("m1", 2)
                .row(550).element("m0", 2).element("m1", 3)
                .row(850).element("m0", 3).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null).datasource("m1", "m1", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(300).element("m0", 1.16666667).element("m1", 2.16666667)
                .row(600).element("m0", 2.16666667).element("m1", 3.16666667)
                .row(900).element("m0",        3.0).element("m1",        4.0)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);

        /*
        for (Row<Measurement> row : actual.collect()) {
            System.err.println(String.format("%d: %s", row.getTimestamp().asSeconds(), row));
        }
        */
        
        assertRDDRowsEqual(expected, actual);
    }


    @Test
    public void testVeryShortSamples() {

        // Samples occur prior to the nearest step interval boundary.
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(000).element("m0", 0).element("m1", 1)
                .row(250).element("m0", 1).element("m1", 2)
                .row(550).element("m0", 2).element("m1", 3)
                .row(850).element("m0", 3).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null).datasource("m1", "m1", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(300).element("m0", 1.16666667).element("m1", 2.16666667)
                .row(600).element("m0", 2.16666667).element("m1", 3.16666667)
                .row(900).element("m0", 3.0).element("m1", 4.0)
                .row(1200).element("m0", Double.NaN).element("m1", Double.NaN)
                .row(1500).element("m0", Double.NaN).element("m1", Double.NaN)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(1500),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }

    @Test
    public void testSkippedSample() {

        // Sample m0 is missing at timestamp 550, (but interval does not exceed heartbeat).
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(  0).element("m0", 0).element("m1", 1)
                .row(250).element("m0", 1).element("m1", 2)
                .row(550).element("m1", 3)
                .row(840).element("m0", 3).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null).datasource("m1", "m1", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(300).element("m0", 1.33333333).element("m1", 2.16666667)
                .row(600).element("m0", 3.00000000).element("m1", 3.16666667)
                .row(900).element("m0", 3.00000000).element("m1", 4.00000000)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);
        
        assertRDDRowsEqual(expected, actual);
    }

    @Test
    public void testManyToOneSamples() {

        // Element interval is less than step size.
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(   0).element("m0", 0).element("m1", 1)
                .row( 300).element("m0", 1).element("m1", 2)
                .row( 600).element("m0", 2).element("m1", 3)
                .row( 900).element("m0", 3).element("m1", 4)
                .row(1200).element("m0", 4).element("m1", 5)
                .row(1500).element("m0", 5).element("m1", 6)
                .row(1800).element("m0", 6).element("m1", 7)
                .row(2100).element("m0", 7).element("m1", 8)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(900))
                .datasource("m0", "m0", Duration.seconds(1800), null).datasource("m1", "m1", Duration.seconds(1800), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row( 900).element("m0", 2).element("m1", 3)
                .row(1800).element("m0", 5).element("m1", 6)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(900),
                Timestamp.fromEpochSeconds(1800),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }


    @Test
    public void testOneToOneSamples() {

        // Samples perfectly correlate to step interval boundaries.
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(   0).element("m0", 0).element("m1", 1)
                .row( 300).element("m0", 1).element("m1", 2)
                .row( 600).element("m0", 2).element("m1", 3)
                .row( 900).element("m0", 3).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null).datasource("m1", "m1", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row(300).element("m0", 1).element("m1", 2)
                .row(600).element("m0", 2).element("m1", 3)
                .row(900).element("m0", 3).element("m1", 4)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }

    @Test
    public void testOneToManySamples() {

        // Actual sample interval is smaller than step size; One sample is mapped to many measurements
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(   0).element("m0", 0).element("m1", 1)
                .row( 900).element("m0", 1).element("m1", 2)
                .row(1800).element("m0", 2).element("m1", 3)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(1000), null).datasource("m1", "m1", Duration.seconds(1000), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row( 300).element("m0", 1).element("m1", 2)
                .row( 600).element("m0", 1).element("m1", 2)
                .row( 900).element("m0", 1).element("m1", 2)
                .row(1200).element("m0", 2).element("m1", 3)
                .row(1500).element("m0", 2).element("m1", 3)
                .row(1800).element("m0", 2).element("m1", 3)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(1800),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }

    @Test
    public void testLongSamples() {

        // Samples occur later-than (after) the step interval.
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(  0).element("m0", 0).element("m1", 1)
                .row(350).element("m0", 1).element("m1", 2)
                .row(650).element("m0", 2).element("m1", 3)
                .row(950).element("m0", 3).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(600), null).datasource("m1", "m1", Duration.seconds(600), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row( 300).element("m0",        1.0).element("m1",        2.0)
                .row( 600).element("m0", 1.83333333).element("m1", 2.83333333)
                .row( 900).element("m0", 2.83333333).element("m1", 3.83333333)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }


    @Test
    public void testHeartbeatOneSample() {

        // Sample interval of 600 seconds (m1) exceeds heartbeat of 601
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(  0).element("m1", 1)
                .row(300).element("m1", 2)
                .row(900).element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m1", "m1", Duration.seconds(601), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row( 300)
                    .element("m1", 2)
                .row( 600)
                    .element("m1", 4)
                .row( 900)
                    .element("m1", 4)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);

    }
    
    @Test
    public void testHeartbeatTwoSamples() {

        // Sample interval of 600 seconds (m1) exceeds heartbeat of 601
        JavaRDD<Row<Sample>> testData = new SampleRDDRowsBuilder(context, new Resource("localhost"), MetricType.GAUGE)
                .row(  0)
                    .element("m0", 0)
                    .element("m1", 1)
                .row(300)
                    .element("m0", 1)
                    .element("m1", 2)
                .row(600)
                    .element("m0", 2)
                    // missing entry for m1
                .row(900)
                    .element("m0", 3)
                    .element("m1", 4)
                .build();

        // Minimal result descriptor
        ResultDescriptor rDescriptor = new ResultDescriptor().step(Duration.seconds(300))
                .datasource("m0", "m0", Duration.seconds(601), null)
                .datasource("m1", "m1", Duration.seconds(601), null);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
                .row( 300)
                    .element("m0", 1)
                    .element("m1", 2)
                .row( 600)
                    .element("m0", 2)
                    .element("m1", 4)
                .row( 900)
                    .element("m0", 3)
                    .element("m1", 4)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(300),
                Timestamp.fromEpochSeconds(900),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);

    }

    @Test
    public void testData() {
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
                .datasource("m1", AVERAGE);

        // Expected results
        JavaRDD<Row<Measurement>> expected = new MeasurementRDDRowsBuilder(context, new Resource("localhost"))
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
                .row(900007200).element("m0", 75000).element("m1", 75000)
                .build();

        JavaRDD<Row<Measurement>> actual = PrimaryData.primaryData(context, testData, new Resource("localhost"),
                Timestamp.fromEpochSeconds(900003600),
                Timestamp.fromEpochSeconds(900007200),
                rDescriptor);

        assertRDDRowsEqual(expected, actual);
    }
}
