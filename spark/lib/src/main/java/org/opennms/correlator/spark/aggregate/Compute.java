package org.opennms.correlator.spark.aggregate;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.Calculation;
import org.opennms.newts.api.query.ResultDescriptor;

import com.google.common.base.Optional;

public class Compute {

    public static class ComputeIt implements Function<Row<Measurement>, Row<Measurement>> {
        private static final long serialVersionUID = 9002609403474880215L;
        private final ResultDescriptor m_resultDescriptor;

        public ComputeIt(ResultDescriptor resultDescriptor) {
            m_resultDescriptor = resultDescriptor;
        }

        @Override
        public Row<Measurement> call(Row<Measurement> row) throws Exception {
            for (Calculation calc : m_resultDescriptor.getCalculations().values()) {
                double v = calc.getCalculationFunction().apply(getValues(row, calc.getArgs()));
                row.addElement(new Measurement(row.getTimestamp(), row.getResource(), calc.getLabel(), v));
            }
            return row;
        }

        private double[] getValues(Row<Measurement> row, String[] names) {
            double[] values = new double[names.length];

            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                Optional<Double> d = parseDouble(name);
                values[i] = d.isPresent() ? d.get() : checkNotNull(row.getElement(name), "Missing measurement; Upstream iterator is bugged").getValue();
            }

            return values;
        }
        
        Optional<Double> parseDouble(String maybeNum) {
            try {
                return Optional.of(Double.parseDouble(maybeNum));
            } catch (NumberFormatException e) {
                return Optional.absent();
            }
        }
    }

    public static JavaRDD<Row<Measurement>> compute(JavaRDD<Row<Measurement>> measurements, ResultDescriptor resultDescriptor) {
        return measurements.map(new ComputeIt(resultDescriptor));
    }
}
