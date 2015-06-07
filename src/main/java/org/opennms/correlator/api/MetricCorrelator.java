package org.opennms.correlator.api;

import java.util.Collection;
import java.util.Date;

public interface MetricCorrelator {

	/**
	 * Returns the top N metrics that correlate with
	 * the given metric over the given range.
	 *
	 * All available metrics are searched.
	 */
	public Collection<Metric> correlator(Metric metric, Date from, Date top, int topN);  
	
}
