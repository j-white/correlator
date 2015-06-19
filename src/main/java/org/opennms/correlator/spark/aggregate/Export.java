package org.opennms.correlator.spark.aggregate;

import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Results.Row;

public class Export {

    public static class FilterColumns implements Function<Row<Measurement>, Row<Measurement>> {
        private static final long serialVersionUID = 9002609403474880215L;
        private final Set<String> m_exports;

        public FilterColumns(Set<String> exports) {
            m_exports = exports;
        }

        @Override
        public Row<Measurement> call(Row<Measurement> row) throws Exception {
            Row<Measurement> result = new Row<>(row.getTimestamp(), row.getResource());

            for (String export : m_exports) {
                result.addElement(getMeasurement(row, export));
            }

            return result;
        }

        private Measurement getMeasurement(Row<Measurement> row, String name) {
            Measurement measurement = row.getElement(name);
            return (measurement != null) ? measurement : getNan(row, name);
        }

        private Measurement getNan(Row<Measurement> row, String name) {
            return new Measurement(row.getTimestamp(), row.getResource(), name, Double.NaN);
        }
    }

    public static JavaRDD<Row<Measurement>> export(JavaRDD<Row<Measurement>> measurements, Set<String> exports) {
        return measurements.map(new FilterColumns(exports));
    }
}
