package org.opennms.correlator.newts;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;

/**
 * Correlation using the Newts API. 
 *
 * @author jwhite
 */
public class NewtsMetricCorrelator implements MetricCorrelator {

	public Collection<Metric> correlator(Metric metric, Date from, Date top,
			int topN) {
		return Collections.emptyList();
	}

}
