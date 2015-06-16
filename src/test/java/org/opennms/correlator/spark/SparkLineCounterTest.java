package org.opennms.correlator.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.spark.SparkLineCounter;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Gauge;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
    "classpath:/applicationContext-newts.xml"
})
public class SparkLineCounterTest {

    @ClassRule
    public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/dataset.cql","newts"));

    @Autowired
    SampleRepository m_sampleRepository;

    @Before
    public void setUp() {
        assertNotNull(m_sampleRepository);
    }

	@Test(timeout=3000000)
	public void canGetNumberOfLines() throws IOException {
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
                {9, 5}
        };
        m_sampleRepository.insert(toSamples("a", "m1", valuesA));

        String hostname = InetAddress.getLocalHost().getHostName();
        SparkLineCounter sparker = new SparkLineCounter("spark://" + hostname + ":7077", "newts");
        Resource resource = new Resource("a");
        Timestamp from = Timestamp.fromEpochMillis(0);
        Timestamp to = Timestamp.fromEpochMillis(9);
        assertEquals(10, sparker.select(resource, Optional.of(from), Optional.of(to)).count());

        Metric metric = new Metric("a", "m1");
        ResultDescriptor descriptor = new ResultDescriptor();
        descriptor.step(1);
        descriptor.datasource(metric.getMetric(), StandardAggregationFunctions.AVERAGE);
        descriptor.export(metric.getMetric());
        //Results<Measurement> measurementsForMetric = m_sampleRepository.select(resource, Optional.of(from), Optional.of(to), descriptor, Duration.millis(2 * 1));
        Results<Measurement> measurementsForMetric = sparker.select(resource, Optional.of(from), Optional.of(to), descriptor, Duration.millis(2 * 1));
        assertEquals(10, measurementsForMetric.getRows().size());
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
