package org.opennms.correlator.newts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricWithCoeff;
import org.opennms.newts.api.Gauge;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.search.Query;
import org.opennms.newts.api.search.SearchResults;
import org.opennms.newts.api.search.Searcher;
import org.opennms.newts.api.search.Term;
import org.opennms.newts.api.search.TermQuery;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class AbstractCorrelatorTest {

    public static String NEWTS_KEYSPACE = "newts";

    @ClassRule
    public static CassandraCQLUnit cassandraUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/dataset.cql", AbstractCorrelatorTest.NEWTS_KEYSPACE));

    @Autowired
    private SampleRepository m_sampleRepository;

    @Autowired
    private Searcher m_searcher;

    private MetricCorrelator m_correlator;

    public abstract MetricCorrelator getCorrelator();

    @Before
    public void setUp() {
        assertNotNull(m_sampleRepository);
        m_correlator = getCorrelator();
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
                {2, 1},
                {3, 0},
                {4, 0},
                {5, 1},
                {6, 0},
                {7, 0},
                {8, 1},
                {9, 0}
        };

        // Add a series whose values are identically 0
        long valuesE[][] = new long[][] {
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

        long values[][] = new long[100][2];
        for (int i = 0; i < 100; i++) {
            values[i][0] = i;
            values[i][1] = 100;
        }

        m_sampleRepository.insert(toSamples("a", "m1", valuesA));
        m_sampleRepository.insert(toSamples("a", "m2", valuesB));
        m_sampleRepository.insert(toSamples("a", "m3", valuesC));
        m_sampleRepository.insert(toSamples("a", "m4", valuesD));
        m_sampleRepository.insert(toSamples("a", "m5", valuesE));

        // Correlate
        Metric metric = new Metric("a", "m1");
        Date from = new Date(0);
        Date to = new Date(10);
        long resolution = 1;
        Collection<MetricWithCoeff> correlatedMetrics = m_correlator.correlate(metric, getAllMetrics(), from, to, resolution, 1);

        // Verify
        assertEquals(1, correlatedMetrics.size());
        assertEquals(new Metric("a", "m2"), correlatedMetrics.iterator().next().getMetric());
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

    private List<Metric> getAllMetrics() {
        List<Metric> metrics = Lists.newLinkedList();
        Query q = new TermQuery(new Term("metric", "true"));
        SearchResults results = m_searcher.search(q);
        for (SearchResults.Result result : results) {
            String resourceId = result.getResource().getId();
            for (String metric : result.getMetrics()) {
                metrics.add(new Metric(resourceId, metric));
            }
            result.getMetrics();
        }
        return metrics;
    }
}
