package org.opennms.correlator.spark.aggregate;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;

public class SparkResultProcessor {

    private final JavaSparkContext m_context;
    private final Resource m_resource;
    private final Timestamp m_start;
    private final Timestamp m_end;
    private final ResultDescriptor m_resultDescriptor;
    private final Duration m_resolution;

    public SparkResultProcessor(JavaSparkContext context, Resource resource, Timestamp start, Timestamp end, ResultDescriptor descriptor, Duration resolution) {
        m_context = checkNotNull(context, "context argument");
        m_resource = checkNotNull(resource, "resource argument");
        m_start = checkNotNull(start, "start argument");
        m_end = checkNotNull(end, "end argument");
        m_resultDescriptor = checkNotNull(descriptor, "result descriptor argument");
        m_resolution = checkNotNull(resolution, "resolution argument");
    }

    public JavaRDD<Row<Measurement>> process(JavaRDD<Row<Sample>> samples) {
        checkNotNull(samples, "samples argument");

        JavaRDD<Row<Sample>> rates = Rate.rate(samples, m_resultDescriptor.getSourceNames());
        JavaRDD<Row<Measurement>> primaryMeasurements = PrimaryData.primaryData(m_context, rates, m_resource, m_start, m_end, m_resultDescriptor);
        JavaRDD<Row<Measurement>> aggregatedMeasurements = Aggregation.aggregate(m_context, primaryMeasurements, m_resource, m_start, m_end, m_resultDescriptor, m_resolution);
        JavaRDD<Row<Measurement>> computedMeasurements = Compute.compute(aggregatedMeasurements, m_resultDescriptor);
        JavaRDD<Row<Measurement>> exportedMeasurements = Export.export(computedMeasurements, m_resultDescriptor.getExports());

        return exportedMeasurements;
    }
}
