package org.opennms.correlator.spark.impl;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import com.typesafe.config.ConfigFactory;

public class SparkMetricCorrelatorIT {

    public static String NEWTS_KEYSPACE = "newts";
    public static String NEWTS_HOST = "45.55.88.223";
    public static int NEWTS_PORT = 9042;

    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local")
                .set("spark.cassandra.connection.host", NEWTS_HOST)
                .set("spark.cassandra.connection.port", "" + NEWTS_PORT);
        context = new JavaSparkContext(conf);
        
        com.datastax.driver.core.ProtocolOptions e;
        com.datastax.driver.core.policies.RetryPolicy f;
    }

    @Test
    public void correlateWithRealData() {
        String config = "{metric: {resource:\"a.b\", metric:\"m\"}, candidates:[{resource:\"a.c\", metric:\"n\"}], from: 0, to: 10, resolution: 1, topN: 10}";
        
        SparkMetricCorrelator sc = new SparkMetricCorrelator();
        List<Map<String, Object>> results = sc.correlate(context.sc(), ConfigFactory.parseString(config));
        assertEquals(results.size(), 1);
        Map<String, Object> result = results.get(0);
        assertEquals(result.get("metric"), "n");
        assertEquals((double)result.get("coefficient"), Double.NaN, 0.1);
    }
}
