package org.opennms.correlator.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.opennms.correlator.spark.Utils.assertRDDRowsEqual;

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
        
        for (Row<Measurement> row : actual.collect()) {
            System.err.println(String.format("%d: %s", row.getTimestamp().asSeconds(), row));
        }

        assertRDDRowsEqual(expected, actual);
    }
}
