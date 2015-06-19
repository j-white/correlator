package org.opennms.correlator.spark;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.api.MetricWithCoeff;
import org.opennms.newts.aggregate.IntervalGenerator;
import org.opennms.newts.aggregate.ResultProcessor;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Results;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.opennms.newts.persistence.cassandra.SchemaConstants;

import org.apache.spark.mllib.stat.Statistics;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SparkMetricCorrelator implements MetricCorrelator {

    private final JavaSparkContext m_context;

    private final String m_keyspace;

    private final SparkContextJavaFunctions m_cassandraContextFunctions;

    private final Duration resourceShard = Duration.seconds(600000);

    @Inject
    public SparkMetricCorrelator(JavaSparkContext context, @Named("newts.keyspace") String keyspace) {
        Preconditions.checkNotNull(context, "context argument");
        Preconditions.checkNotNull(keyspace, "keyspace argument");
        m_context = context;
        m_keyspace = keyspace;
        m_cassandraContextFunctions = CassandraJavaUtil.javaFunctions(m_context);
    }

    @Override
    public Collection<MetricWithCoeff> correlate(Metric metric, List<Metric> candidates,
            Date from, Date to, long resolution, int topN) {
        int durationMultiplier = 3;
        Resource resource = new Resource(metric.getResource());
        Timestamp start = Timestamp.fromDate(from);
        Timestamp end = Timestamp.fromDate(to);

        // Retrieve the measurements for our first metric
        ResultDescriptor descriptor = new ResultDescriptor();
        descriptor.step(resolution);
        descriptor.datasource(metric.getMetric(), StandardAggregationFunctions.AVERAGE);
        descriptor.export(metric.getMetric());
        Results<Measurement> measurementsForMetric = select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(durationMultiplier * resolution));
        // Iterate over all of the other metrics
        List<MetricWithCoeff> metricsWithCoeffs = Lists.newArrayList();
        for (Metric candidate : candidates) {
            if (metric.equals(candidate)) {
                continue;
            }

            // Retrieve the measurements
            resource = new Resource(candidate.getResource());
            descriptor = new ResultDescriptor();
            descriptor.step(resolution);
            descriptor.datasource(candidate.getMetric(), StandardAggregationFunctions.AVERAGE);
            descriptor.export(candidate.getMetric());
            Results<Measurement> measurementsForOtherMetric = select(resource, Optional.of(start), Optional.of(end), descriptor, Duration.millis(durationMultiplier * resolution));

            // Perform the correlation
            double correlationCoefficient = correlate(metric.getMetric(), candidate.getMetric(), measurementsForMetric, measurementsForOtherMetric);

            // Store the results
            MetricWithCoeff metricWithCoeff = new MetricWithCoeff(candidate, correlationCoefficient);
            metricsWithCoeffs.add(metricWithCoeff);
        }

        // Select the Top N metrics
        Collections.sort(metricsWithCoeffs);
        return metricsWithCoeffs.subList(0, topN);
    }

    public double correlate(String name1, String name2, Results<Measurement> results1, Results<Measurement> results2) {
        Preconditions.checkArgument(results1.getRows().size() == results2.getRows().size());
        JavaDoubleRDD seriesX = toDoubleRDD(name1, results1);
        JavaDoubleRDD seriesY = toDoubleRDD(name2, results2);
        return Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
    }

    private JavaDoubleRDD toDoubleRDD(String metricName, Results<Measurement> measurements) {
        List<Double> doubles = measurements.getRows().stream()
                    .map(row -> row.getElement(metricName).getValue())
                    .collect(Collectors.toList());
        return m_context.parallelizeDoubles(doubles);
    }

	public Results<Measurement> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end, ResultDescriptor descriptor, Duration resolution) {
	    // Select the the appropriate rows from the database
	    JavaRDD<CassandraRow> rows = select(resource, start, end);
	    // Group the rows by timestamp
	    JavaPairRDD<Timestamp, Iterable<CassandraRow>> rowsKeyedByTimestamp = rows.groupBy(new SparkUtils.TimestampExtractor());
	    // Sort the timestamps in ascending order
	    rowsKeyedByTimestamp = rowsKeyedByTimestamp.sortByKey();
	    // Convert the grouped rows into sample rows
	    JavaRDD<Row<Sample>> sampleRows = rowsKeyedByTimestamp.map(new SparkUtils.CassandraRowsToSampleRows(resource));

	    // FIXME: No more RDD :(
	    // Calculate the measurements
	    return new ResultProcessor(resource, start.get(), end.get(), descriptor, resolution).process(sampleRows.collect().iterator());
	}

	public JavaRDD<CassandraRow> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end) {
        Timestamp lower = start.get().stepFloor(resourceShard);
        Timestamp upper = end.get().stepFloor(resourceShard);

        CassandraJavaRDD<CassandraRow> samples = m_cassandraContextFunctions.cassandraTable(m_keyspace, SchemaConstants.T_SAMPLES);
        JavaRDD<CassandraRow> rows = null;
        for (Timestamp partition : new IntervalGenerator(lower, upper, resourceShard)) {
            JavaRDD<CassandraRow> nextRows = samples.select(SchemaConstants.F_METRIC_NAME, SchemaConstants.F_VALUE, SchemaConstants.F_ATTRIBUTES, SchemaConstants.F_COLLECTED)
                    .where("partition = ? AND resource = ? AND collected_at >= ? AND collected_at <= ?",
                            (int) partition.asSeconds(), resource.getId(), start.get().asDate(), end.get().asDate());
            if (rows == null) {
                rows = nextRows;
            } else {
                rows = rows.union(nextRows);
            }
        }

        return rows;
	}
}
