package org.opennms.correlator.spark.aggregate;

import org.opennms.newts.api.query.ResultDescriptor.BinaryFunction;

/**
 * Example binary functions - needs to be here for tests on Spark cluster to work.
 *
 * @author jesse
 */
public class Sum implements BinaryFunction {
    private static final long serialVersionUID = 955989572094690429L;

    @Override
    public double apply(double a, double b) {
        return a + b;
    }
}
