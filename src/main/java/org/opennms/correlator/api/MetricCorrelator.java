package org.opennms.correlator.api;

import java.util.Collection;
import java.util.Date;

public interface MetricCorrelator {

	/**
	 * Returns the top N metrics that correlate with
	 * the given metric over the given range.
	 *
	 * All available metrics are searched.
	 *
	 * The returned collection is sorted, starting
	 * with the metric that correlates the most.
	 */
	public Collection<Metric> correlate(Metric metric, Date from, Date to, long resolution, int topN);  
	
}
