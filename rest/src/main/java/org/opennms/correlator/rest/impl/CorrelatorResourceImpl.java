package org.opennms.correlator.rest.impl;

import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricProvider;
import org.opennms.correlator.api.MetricWithCoeff;
import org.opennms.correlator.rest.api.CorrelatorResource;
import org.opennms.correlator.rest.api.MetricAndCoefficientDTO;
import org.opennms.correlator.rest.api.MetricDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorrelatorResourceImpl implements CorrelatorResource {

    private static final Logger LOG = LoggerFactory
            .getLogger(CorrelatorResourceImpl.class);

    private MetricCorrelator m_metricCorrelator;
    private MetricProvider m_metricProvider;

    @Override
    public Collection<MetricAndCoefficientDTO> correlate(String resource,
            String metric, Long from, Long to, Long resolution,
            Integer topN) {
        // Grab the complete list of available metrics
        List<Metric> allAvailableMetrics = m_metricProvider.getAllMetrics();
        // Correlate
        return correlateMetrics(resource, metric, from, to, resolution, topN,
                allAvailableMetrics);
    }

    @Override
    public Collection<MetricAndCoefficientDTO> correlate(String resource,
            String metric, Long from, Long to, Long resolution,
            Integer topN, List<MetricDTO> candidateMetrics) {
        // Convert the DTOs
        List<Metric> metrics = candidateMetrics.stream()
                .map(m -> new Metric(m.getResource(), m.getMetric()))
                .collect(Collectors.toList());
        // Correlate
        return correlateMetrics(resource, metric, from, to, resolution, topN,
                metrics);
    }

    private Collection<MetricAndCoefficientDTO> correlateMetrics(
            String resource, String metric, Long from, Long to,
            Long resolution, Integer topN, List<Metric> candidateMetrics) {

        if (resource == null || metric == null || resolution == null || topN == null) {
            throw new WebApplicationException(Response
                    .status(HttpURLConnection.HTTP_BAD_REQUEST)
                    .entity("Missing mandatory parameter.").build());
        }

        long start = from != null ? from : new Date().getTime() - 24*60*60*60*1000; // 24 hours ago
        long end = to != null ? to : new Date().getTime(); // now

        LOG.info("Correlating {}/{} with {} metrics.", resource, metric, candidateMetrics.size());
        Collection<MetricWithCoeff> metricsWithCoeffs = m_metricCorrelator.correlate(new Metric(resource, metric), candidateMetrics, new Date(start), new Date(end), resolution, topN);
        LOG.info("Correlate returned {} metrics.", metricsWithCoeffs.size());
        return metricsWithCoeffs.stream()
                .map(m -> new MetricAndCoefficientDTO(m.getMetric()
                        .getResource(), m.getMetric().getMetric(), m.getCoeff()))
                .collect(Collectors.toList());
    }

    public void setMetricCorrelator(MetricCorrelator metricCorrelator) {
        m_metricCorrelator = metricCorrelator;
    }

    public MetricCorrelator getMetricCorrelator() {
        return m_metricCorrelator;
    }

    public void setMetricProvider(MetricProvider metricProvider) {
        m_metricProvider = metricProvider;
    }

    public MetricProvider getMetricProvider() {
        return m_metricProvider;
    }
}
