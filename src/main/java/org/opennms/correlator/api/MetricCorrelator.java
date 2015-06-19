package org.opennms.correlator.api;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public interface MetricCorrelator {

	/**
	 * Returns the top N metrics from the list
	 * of candidates that correlate with
	 * the given metric over the given range.
	 *
	 * The returned collection is sorted, starting
	 * with the metric that correlates the most.
	 */
	public Collection<MetricWithCoeff> correlate(Metric metric, List<Metric> candidates, Date from, Date to, long resolution, int topN);  
	
}
