package org.opennms.correlator.api;

import java.io.Serializable;

import com.google.common.base.Objects;

public class MetricWithCoeff implements Comparable<MetricWithCoeff>, Serializable {
    private static final long serialVersionUID = 1115045583691907741L;
    private final Metric m_metric;
    private final double m_coeff;

    public MetricWithCoeff(Metric metric, double coeff) {
        m_metric = metric;
        m_coeff = coeff;
    }

    public Metric getMetric() {
        return m_metric;
    }

    public double getCoeff() {
        return m_coeff;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
           .add("metric", m_metric)
           .add("coeff", m_coeff)
           .toString();
    }

    public int compareTo(MetricWithCoeff other) {
        return Double.valueOf(Math.abs(other.m_coeff)).compareTo(Math.abs(m_coeff));
    }
}
