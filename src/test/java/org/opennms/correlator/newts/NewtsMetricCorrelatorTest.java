package org.opennms.correlator.newts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.correlator.api.Metric;
import org.opennms.newts.api.Gauge;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
	"classpath:/applicationContext-newts.xml"
})
public class NewtsMetricCorrelatorTest {

    @ClassRule
    public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/dataset.cql","newts"));

	@Autowired
	NewtsMetricCorrelator m_correlator;

	@Autowired
	SampleRepository m_sampleRepository;

	@Before
	public void setUp() {
		assertNotNull(m_sampleRepository);
		assertNotNull(m_correlator);
	}

	@Test
	public void canCorrelate() {
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

		long valuesB[][] = new long[][] {
				{0, 0},
				{1, 0},
				{2, 0},
				{3, 0},
				{4, 0},
				{5, 0},
				{6, 0},
				{7, 0},
				{8, 0},
				{9, 0}
		};

		long valuesC[][] = new long[][] {
				{0, 0},
				{1, 1},
				{2, 2},
				{3, 3},
				{4, 4},
				{5, 5},
				{6, 6},
				{7, 7},
				{8, 8},
				{9, 9}
		};

		long valuesD[][] = new long[][] {
				{0, 0},
				{1, 0},
				{2, 0},
				{3, 0},
				{4, 0},
				{5, 0},
				{6, 0},
				{7, 0},
				{8, 0},
				{9, 0}
		};

		m_sampleRepository.insert(toSamples("a", "m1", valuesA));
		m_sampleRepository.insert(toSamples("a", "m2", valuesB));
		m_sampleRepository.insert(toSamples("a", "m3", valuesC));
		m_sampleRepository.insert(toSamples("a", "m4", valuesD));

		// Correlate
		Metric metric = new Metric("a", "m1");
		Date from = new Date(0);
		Date to = new Date(10);
		long resolution = 1;
		Collection<Metric> correlatedMetrics = m_correlator.correlate(metric, from, to, resolution, 1);

		// Verify
		assertEquals(1, correlatedMetrics.size());
		assertEquals(new Metric("a", "m3"), correlatedMetrics.iterator().next());
	}

	private static List<Sample> toSamples(String resourceId, String metricName, long[][] values) {
		Resource resource = new Resource(resourceId);
		MetricType type = MetricType.GAUGE;

		List<Sample> samples = Lists.newArrayList();
		for (int i = 0; i < values.length; i++) {
			samples.add(new Sample(Timestamp.fromEpochSeconds(values[i][1]),
					resource, metricName, type,
					new Gauge(values[i][0])));
		}

		return samples;
	}
}
