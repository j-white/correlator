package org.opennms.correlator.api;

import java.io.Serializable;

import com.google.common.base.Objects;

public class Metric implements Serializable {
	private static final long serialVersionUID = -8687971948177751021L;

	private final String m_resource;

	private final String m_metric;

	public Metric(String resource, String metric) {
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_metric == null) ? 0 : m_metric.hashCode());
		result = prime * result
				+ ((m_resource == null) ? 0 : m_resource.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Metric other = (Metric) obj;
		if (m_metric == null) {
			if (other.m_metric != null)
				return false;
		} else if (!m_metric.equals(other.m_metric))
			return false;
		if (m_resource == null) {
			if (other.m_resource != null)
				return false;
		} else if (!m_resource.equals(other.m_resource))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
		   .add("resource", m_resource)
	       .add("metric", m_metric)
	       .toString();
	}
}
