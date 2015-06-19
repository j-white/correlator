package org.opennms.correlator.api;

import com.google.common.base.Objects;

public class MetricWithCoeff implements Comparable<MetricWithCoeff> {
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
    public String toString() {
        return Objects.toStringHelper(this)
           .add("metric", m_metric)
           .add("coeff", m_coeff)
           .toString();
    }

    public int compareTo(MetricWithCoeff other) {
        return Double.valueOf(other.m_coeff).compareTo(m_coeff);
    }
}
