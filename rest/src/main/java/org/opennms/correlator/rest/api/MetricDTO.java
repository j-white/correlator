package org.opennms.correlator.rest.api;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.base.Objects;

public class MetricDTO {

    private final String m_resource;

    private final String m_metric;

    @JsonCreator
    public MetricDTO(@JsonProperty("resource") String resource, @JsonProperty("metric") String metric) {
        m_resource = resource;
        m_metric = metric;
    }

    public String getResource() {
        return m_resource;
    }

    public String getMetric() {
        return m_metric;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final MetricDTO other = (MetricDTO) obj;
        return Objects.equal(m_resource, other.m_resource)
            && Objects.equal(m_metric, other.m_metric);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                m_resource, m_metric);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
           .add("resource", m_resource)
           .add("metric", m_metric)
           .toString();
    }
}
