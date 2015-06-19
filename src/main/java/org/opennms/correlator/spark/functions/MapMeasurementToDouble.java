package org.opennms.correlator.spark.functions;

import java.util.stream.Collectors;

import org.apache.spark.api.java.function.DoubleFunction;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Results.Row;

public class MapMeasurementToDouble implements DoubleFunction<Row<Measurement>> {
    private static final long serialVersionUID = -6621954661942703743L;

    private final String m_metricName;

    public MapMeasurementToDouble(String metricName) {
        m_metricName = metricName;
    }

    @Override
    public double call(Row<Measurement> row) throws Exception {
        Measurement m = row.getElement(m_metricName);
        if (m == null) {
            String availableMetricNames = row.getElements().stream().map(e -> e.getName())
                .collect(Collectors.joining(","));
            throw new Exception(m_metricName + " is not available in the given row. Available metrics: " + availableMetricNames);
        }
        return row.getElement(m_metricName).getValue();
    }
}
