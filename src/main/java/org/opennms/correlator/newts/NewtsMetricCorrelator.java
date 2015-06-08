package org.opennms.correlator.newts;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.opennms.newts.api.search.Query;
import org.opennms.newts.api.search.Searcher;
import org.opennms.newts.api.search.Term;
import org.opennms.newts.api.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Correlation using the Newts API. 
 *
 * @author jwhite
 */
public class NewtsMetricCorrelator implements MetricCorrelator {

	private static final Logger LOG = LoggerFactory.getLogger(NewtsMetricCorrelator.class);

	@Inject
	private SampleRepository m_sampleRepository;

	@Inject
	private Searcher m_searcher;

	private PearsonsCorrelation pc = new PearsonsCorrelation();	

	public double correlate(String name1, String name2, Results<Measurement> results1, Results<Measurement> results2) {
		Preconditions.checkArgument(results1.getRows().size() == results2.getRows().size());
		int N = results1.getRows().size();
		
		double x[] = new double[N];
		double y[] = new double[N];

		int i = 0;
		for (Row<Measurement> row : results1.getRows()) {
			x[i] = row.getElement(name1).getValue();
			// Replace NaNs with 0s
			if (Double.isNaN(x[i])) { x[i] = 0; }
			i++;
		}

		i = 0;
		for (Row<Measurement> row : results2.getRows()) {
			y[i] = row.getElement(name2).getValue();
			// Replace NaNs with 0s
			if (Double.isNaN(y[i])) { y[i] = 0; }
			i++;
		}

		LOG.info("MOO2: x: {}, y: {}", Arrays.toString(x), Arrays.toString(y));
		return pc.correlation(x, y);
	}

	private List<Metric> getAllMetrics() {
		Query q = new TermQuery(new Term("_parent0", ""));
		m_searcher.search(q);

		return Lists.newArrayList(
			new Metric("a", "m1"),
			new Metric("a", "m2"),
			new Metric("a", "m3"),
			new Metric("a", "m4")
		);
	}

	private static class MetricWithCoeff implements Comparable<MetricWithCoeff> {
		private final Metric m_metric;
		private final double m_coeff;
		
		public MetricWithCoeff(Metric metric, double coeff) {
			m_metric = metric;
			m_coeff = coeff;
		}

		public Metric getMetric() {
			return m_metric;
		}

		@Override
		public int compareTo(MetricWithCoeff other) {
			return Double.valueOf(m_coeff).compareTo(Double.valueOf(other.m_coeff));
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this)
		       .add("metric", m_metric)
		       .add("coeff", m_coeff)
		       .toString();
		}
	}

	public Collection<Metric> correlate(Metric metric,
			Date from, Date to, long resolution,
			int topN) {
		
		Resource resource = new Resource(metric.getResource());
		Timestamp start = Timestamp.fromDate(from);
		Timestamp end = Timestamp.fromDate(to);

		ResultDescriptor descriptor = new ResultDescriptor();
		descriptor.step(resolution);
		descriptor.datasource(metric.getMetric(), StandardAggregationFunctions.AVERAGE);
		descriptor.export(metric.getMetric());

		Results<Measurement> measurementsForMetric = m_sampleRepository.select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(2 * resolution));

		List<MetricWithCoeff> metricsWithCoeffs = Lists.newArrayList();
		for (Metric otherMetric : getAllMetrics()) {
			if (metric.equals(otherMetric)) {
				continue;
			}

			resource = new Resource(otherMetric.getResource());
			descriptor = new ResultDescriptor();
			descriptor.step(resolution);
			descriptor.datasource(otherMetric.getMetric(), StandardAggregationFunctions.AVERAGE);
			descriptor.export(otherMetric.getMetric());

			Results<Measurement> measurementsForOtherMetric = m_sampleRepository.select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(2 * resolution));

			double correlationCoefficient = correlate(metric.getMetric(), otherMetric.getMetric(), measurementsForMetric, measurementsForOtherMetric);
			LOG.info("MOO3: Correlation returned {}", correlationCoefficient);
			
			MetricWithCoeff metricWithCoeff = new MetricWithCoeff(otherMetric, correlationCoefficient);
			metricsWithCoeffs.add(metricWithCoeff);

			LOG.info("MOO1: {} - {}", metric, metricWithCoeff);
		}

		Collections.sort(metricsWithCoeffs);
		return metricsWithCoeffs.subList(0, Math.min(topN, metricsWithCoeffs.size())).stream()
			.map(m -> m.getMetric())
			.collect(Collectors.toList());
	}
}
