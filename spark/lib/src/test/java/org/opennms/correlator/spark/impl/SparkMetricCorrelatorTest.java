package org.opennms.correlator.spark.impl;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.correlator.newts.AbstractCorrelatorTest;
import org.opennms.newts.api.Gauge;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
    "classpath:/applicationContext-newts.xml"
})
public class SparkMetricCorrelatorTest {

    public static String NEWTS_KEYSPACE = "newts";

    @ClassRule
    public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/dataset.cql", AbstractCorrelatorTest.NEWTS_KEYSPACE));

    @Autowired
    private SampleRepository m_sampleRepository;

    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.port", "9142");
        context = new JavaSparkContext(conf);
    }

    @Test
    public void testIt() {
        // Define and insert samples
        long valuesA[][] = new long[][] {
                {0, 5},
                {1, 5},
                {2, 5},
                {3, 5},
                {4, 5},
                {5, 5},
                {6, 5},
                {7, 5},
                {8, 5},
                {9, 5},
                {9, 5}
        };

        long valuesB[][] = new long[][] {
                {0, 1},
                {1, 0},
                {2, 1},
                {3, 0},
                {4, 1},
                {5, 0},
                {6, 1},
                {7, 0},
                {8, 1},
                {9, 0}
        };
        
        m_sampleRepository.insert(toSamples("a", "m1", valuesA));
        m_sampleRepository.insert(toSamples("a", "m2", valuesB));

        String config = "{" +
                "metric={resource=\"a\", metric=\"m1\"}, " +
                "candidates=[{resource=\"a\", metric=\"m2\"}], " +
                "from: 0, to: 10, resolution: 1, topN: 1" +
                "}";

        SparkMetricCorrelator sc = new SparkMetricCorrelator();
        List<Map<String, Object>> results = sc.correlate(context.sc(), ConfigFactory.parseString(config));
        assertEquals(results.size(), 1);
        Map<String, Object> result = results.get(0);
        assertEquals(result.get("metric"), "m2");
        assertEquals((double)result.get("coefficient"), Double.NaN, 0.1);
    }

    private static List<Sample> toSamples(String resourceId, String metricName, long[][] values) {
        Map<String, String> attributes = Maps.newHashMap();
        // Tag all metrics
        attributes.put("metric", "true");
        Resource resource = new Resource(resourceId, Optional.of(attributes));
        MetricType type = MetricType.GAUGE;

        List<Sample> samples = Lists.newArrayList();
        for (int i = 0; i < values.length; i++) {
            samples.add(new Sample(Timestamp.fromEpochMillis(values[i][0]),
                    resource, metricName, type,
                    new Gauge(values[i][1])));
        }

        return samples;
    }
}
