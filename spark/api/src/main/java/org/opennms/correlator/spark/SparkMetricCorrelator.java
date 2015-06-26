package org.opennms.correlator.spark;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricWithCoeff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SparkMetricCorrelator implements MetricCorrelator {

    private static final Logger LOG = LoggerFactory.getLogger(SparkMetricCorrelator.class);

    private final String m_jobServerUrl;

    @Inject
    public SparkMetricCorrelator(@Named("org.opennms.correlator.spark.jobServerUrl") String jobServerUrl) {
        m_jobServerUrl = jobServerUrl;
    }

    @Override
    public Collection<MetricWithCoeff> correlate(Metric metric, List<Metric> candidates,
            Date from, Date to, long resolution, int topN) {
        
        LOG.info("Submitting job to {}", m_jobServerUrl);

        return Lists.newArrayList();
    }
}
