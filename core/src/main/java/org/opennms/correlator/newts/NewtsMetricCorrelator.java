package org.opennms.correlator.newts;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricWithCoeff;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.SampleRepository;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Correlation using the Newts API. 
 *
 * @author jwhite
 */
public class NewtsMetricCorrelator implements MetricCorrelator {

    private static final int DURATION_MULTIPLIER = 3;

	private static final Logger LOG = LoggerFactory.getLogger(NewtsMetricCorrelator.class);

	@Inject
	private SampleRepository m_sampleRepository;

	private PearsonsCorrelation pc = new PearsonsCorrelation();	
	
	private SpearmansCorrelation sc = new SpearmansCorrelation();

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

		// TODO: Ignore series that are close to [0,0,0,0,0...0] if this series is not
		
		double coeff = pc.correlation(x, y);
		LOG.debug("x: {}, y: {} coeff: {}", Arrays.toString(x), Arrays.toString(y), coeff);
		return coeff;
	}

	public Collection<MetricWithCoeff> correlate(Metric metric, List<Metric> candidates,
			Date from, Date to, long resolution,
			int topN) {

		Resource resource = new Resource(metric.getResource());
		Timestamp start = Timestamp.fromDate(from);
		Timestamp end = Timestamp.fromDate(to);

		// Retrieve the measurements for our first metric
		ResultDescriptor descriptor = new ResultDescriptor();
		descriptor.step(resolution);
		descriptor.datasource(metric.getMetric(), StandardAggregationFunctions.AVERAGE);
		descriptor.export(metric.getMetric());
		Results<Measurement> measurementsForMetric = m_sampleRepository.select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(DURATION_MULTIPLIER * resolution));

		// Calculate the coefficients in parallel
		List<MetricWithCoeff> metricsWithCoeffs = candidates.parallelStream()
		    .filter(c -> !metric.equals(c)) // don't correlate the metric with itself
		    .map(c -> correlate(metric, c, measurementsForMetric, start, end, resolution))
		    .filter(r -> !Double.isNaN(r.getCoeff())) // skip results that are NaN
		    .collect(Collectors.toList());

		// Select the Top N metrics
		Collections.sort(metricsWithCoeffs);
		return metricsWithCoeffs.subList(0, Math.min(metricsWithCoeffs.size(), topN));
	}
	
	private MetricWithCoeff correlate(Metric target, Metric candidate, Results<Measurement> targetMeasurements, Timestamp start, Timestamp end, long resolution) {
	    // Retrieve the measurements
	    Resource resource = new Resource(candidate.getResource());
	    ResultDescriptor descriptor = new ResultDescriptor();
        descriptor.step(resolution);
        descriptor.datasource(candidate.getMetric(), StandardAggregationFunctions.AVERAGE);
        descriptor.export(candidate.getMetric());
        Results<Measurement> measurementsForOtherMetric = m_sampleRepository.select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(DURATION_MULTIPLIER * resolution));

        // Perform the correlation
        double correlationCoefficient = correlate(target.getMetric(), candidate.getMetric(), targetMeasurements, measurementsForOtherMetric);
        return new MetricWithCoeff(candidate, correlationCoefficient);
	}

	public void setSampleRepository(SampleRepository sampleRepository) {
	    m_sampleRepository = sampleRepository;
	}

	public SampleRepository getSampleRepository() {
	    return m_sampleRepository;
	}
}
