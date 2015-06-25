package org.opennms.correlator.api;

import java.util.List;

public interface MetricProvider {

    public List<Metric> getAllMetrics();

}
