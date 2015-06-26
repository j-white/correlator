package org.opennms.correlator.spark;

import java.util.Collections;
import java.util.List;

import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricProvider;

public class SparkMetricProvider implements MetricProvider {

    @Override
    public List<Metric> getAllMetrics() {
        return Collections.emptyList();
    }

}
