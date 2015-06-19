package org.opennms.correlator.spark;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricWithCoeff;
import org.opennms.correlator.spark.functions.MapMeasurementToDouble;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.apache.spark.mllib.stat.Statistics;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SparkMetricCorrelator implements MetricCorrelator {

    private static final String CORRELATION_TYPE = "pearson";

    private final static int DURATION_MULTIPLIER = 3;

    private final SparkSampleReader m_sparkSampleReader;

    @Inject
    public SparkMetricCorrelator(SparkSampleReader sparkSampleReader) {
        m_sparkSampleReader = sparkSampleReader;
    }

    @Override
    public Collection<MetricWithCoeff> correlate(Metric metric, List<Metric> candidates,
            Date from, Date to, long resolution, int topN) {

        // Group the metric names by resource id
        Map<String, Set<String>> metricsByResource = Maps.newHashMap();
        metricsByResource.put(metric.getResource(), Sets.newHashSet(metric.getMetric()));
        for (Metric m : candidates) {
            Set<String> metrics = metricsByResource.get(m.getResource());
            if (metrics == null) {
                metrics = Sets.newHashSet(m.getMetric());
                metricsByResource.put(m.getResource(), metrics);
            } else {
                metrics.add(m.getMetric());
            }
        }

        // Build the RDDs containing the measurements for all of the required metrics
        // Group these by resource id
        Map<String, JavaRDD<Row<Measurement>>> measurementsByResource = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : metricsByResource.entrySet()) {
            Resource resource = new Resource(entry.getKey());
            Timestamp start = Timestamp.fromDate(from);
            Timestamp end = Timestamp.fromDate(to);

            ResultDescriptor descriptor = new ResultDescriptor();
            descriptor.step(resolution);
            for (String metricName : entry.getValue()) {
                descriptor.datasource(metricName, StandardAggregationFunctions.AVERAGE);
                descriptor.export(metricName);
            }

            JavaRDD<Row<Measurement>> measurements = m_sparkSampleReader.select(resource, Optional.of(start), Optional.of(end),
                    descriptor, Duration.millis(DURATION_MULTIPLIER * resolution)).cache();
            measurementsByResource.put(resource.getId(), measurements);
        }

        JavaRDD<Row<Measurement>> measurementsForPrimaryMetric = measurementsByResource.get(metric.getResource());
        JavaDoubleRDD seriesForPrimaryMetric = measurementsForPrimaryMetric.mapToDouble(new MapMeasurementToDouble(metric.getMetric()));

        // Iterate over all of the candidate metrics
        List<MetricWithCoeff> metricsWithCoeffs = Lists.newArrayList();
        for (Metric candidate : candidates) {
            // Skip the candidate if it's the same as our primary metric
            if (metric.equals(candidate)) {
                continue;
            }

            // Perform pair-wise correlation
            JavaRDD<Row<Measurement>> measurementsForCandidateMetric = measurementsByResource.get(candidate.getResource());
            JavaDoubleRDD seriesForCandidateMetric = measurementsForCandidateMetric.mapToDouble(new MapMeasurementToDouble(candidate.getMetric()));
            double correlationCoefficient = Statistics.corr(seriesForPrimaryMetric.srdd(), seriesForCandidateMetric.srdd(), CORRELATION_TYPE);

            // Stash the results
            MetricWithCoeff metricWithCoeff = new MetricWithCoeff(candidate, correlationCoefficient);
            metricsWithCoeffs.add(metricWithCoeff);
        }

        // Select the Top N metrics
        Collections.sort(metricsWithCoeffs);
        return metricsWithCoeffs.subList(0, topN);
    }
}
