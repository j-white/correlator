package org.opennms.correlator.spark.aggregate;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.newts.aggregate.Aggregation;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;

import com.google.common.collect.Lists;

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
        JavaRDD<Row<Measurement>> primaryData = PrimaryData.primaryData(m_context, rates, m_resource, m_start, m_end, m_resultDescriptor);

        Iterator<Row<Measurement>> it = primaryData.collect().iterator();
        Aggregation aggregation = new Aggregation(m_resource, m_start, m_end, m_resultDescriptor, m_resolution, it);

        /*
        // Spark -> Current JVM (slow)
        Iterator<Row<Sample>> it = rates.collect().iterator();
        PrimaryData primaryData = new PrimaryData(m_resource, m_start.minus(m_resolution), m_end, m_resultDescriptor, it);
        Aggregation aggregation = new Aggregation(m_resource, m_start, m_end, m_resultDescriptor, m_resolution, primaryData);
        */
        // Current JVM -> Spark (slow)
        Results<Measurement> theMeasurements = new Results<Measurement>();
        for (Row<Measurement> row : aggregation) {
            theMeasurements.addRow(row);
        }

        JavaRDD<Row<Measurement>> measurements =  m_context.parallelize(Lists.newLinkedList(theMeasurements.getRows()));
        JavaRDD<Row<Measurement>> computedMeasurements = Compute.compute(measurements, m_resultDescriptor);
        JavaRDD<Row<Measurement>> exportedMeasurements = Export.export(computedMeasurements, m_resultDescriptor.getExports());

        return exportedMeasurements;
    }
}
