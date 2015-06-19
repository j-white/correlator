package org.opennms.correlator.spark;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.newts.aggregate.ResultProcessor;
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
        // Spark -> Current JVM (slow)
        Results<Measurement> results = new ResultProcessor(m_resource, m_start, m_end, m_resultDescriptor, m_resolution).process(samples.collect().iterator());
        // Current JVM -> Spark (slow)
        return m_context.parallelize(Lists.newLinkedList(results.getRows()));
    }
}
