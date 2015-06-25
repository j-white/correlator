package org.opennms.correlator.rest.api;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.base.Objects;

public class MetricAndCoefficientDTO {

    private final String m_resource;

    private final String m_metric;

    private final double m_coefficient;

    @JsonCreator
    public MetricAndCoefficientDTO(@JsonProperty("resource") String resource,@JsonProperty("metric") String metric,@JsonProperty("coefficient") Double coefficient) {
        m_resource = resource;
        m_metric = metric;
        m_coefficient = coefficient;
    }

    public String getResource() {
        return m_resource;
    }

    public String getMetric() {
        return m_metric;
    }

    public double getCoefficient() {
        return m_coefficient;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final MetricAndCoefficientDTO other = (MetricAndCoefficientDTO) obj;
        return Objects.equal(m_resource, other.m_resource)
            && Objects.equal(m_metric, other.m_metric)
            && Objects.equal(m_coefficient, other.m_coefficient);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                m_resource, m_metric, m_coefficient);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
           .add("resource", m_resource)
           .add("metric", m_metric)
           .add("coefficient", m_coefficient)
           .toString();
    }
}
