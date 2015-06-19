package org.opennms.correlator.spark;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.correlator.spark.aggregate.SparkResultProcessor;
import org.opennms.correlator.spark.functions.CassandraRowsToSampleRows;
import org.opennms.correlator.spark.functions.MapRowToTimestamp;
import org.opennms.newts.aggregate.IntervalGenerator;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.persistence.cassandra.SchemaConstants;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Implements the select() functions from {@link CassandraSampleRepository}
 * using Spark.
 *
 * @author jwhite
 */
public class SparkSampleReader {

    private final JavaSparkContext m_context;

    private final String m_keyspace;

    private final SparkContextJavaFunctions m_cassandraContextFunctions;

    private final Duration resourceShard = Duration.seconds(600000);

    @Inject
    public SparkSampleReader(JavaSparkContext context, @Named("newts.keyspace") String keyspace) {
        Preconditions.checkNotNull(context, "context argument");
        Preconditions.checkNotNull(keyspace, "keyspace argument");
        m_context = context;
        m_keyspace = keyspace;
        m_cassandraContextFunctions = CassandraJavaUtil.javaFunctions(m_context);
    }

    public JavaRDD<Row<Sample>> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end) {

        Timestamp upper = end.isPresent() ? end.get() : Timestamp.now();
        Timestamp lower = start.isPresent() ? start.get() : upper.minus(Duration.seconds(86400));

        Timestamp lowerShard = upper.stepFloor(resourceShard);
        Timestamp upperShard = lower.stepFloor(resourceShard);

        // Combine the rows from the all the partitions
        CassandraJavaRDD<CassandraRow> samples = m_cassandraContextFunctions.cassandraTable(m_keyspace, SchemaConstants.T_SAMPLES);
        JavaRDD<CassandraRow> rows = null;
        for (Timestamp partition : new IntervalGenerator(lowerShard, upperShard, resourceShard)) {
            JavaRDD<CassandraRow> nextRows = samples.select(SchemaConstants.F_METRIC_NAME, SchemaConstants.F_VALUE, SchemaConstants.F_ATTRIBUTES, SchemaConstants.F_COLLECTED)
                    .where("partition = ? AND resource = ? AND collected_at >= ? AND collected_at <= ?",
                            (int) partition.asSeconds(), resource.getId(), lower.asDate(), upper.asDate());
            if (rows == null) {
                rows = nextRows;
            } else {
                rows = rows.union(nextRows);
            }
        }

        // Group the rows by timestamp
        JavaPairRDD<Timestamp, Iterable<CassandraRow>> rowsKeyedByTimestamp = rows.groupBy(new MapRowToTimestamp());

        // Sort the timestamps in ascending order
        rowsKeyedByTimestamp = rowsKeyedByTimestamp.sortByKey();

        // Convert the grouped rows into sample rows
        return rowsKeyedByTimestamp.map(new CassandraRowsToSampleRows(resource));
    }

    public JavaRDD<Row<Measurement>> select(Resource resource,
            Optional<Timestamp> start, Optional<Timestamp> end,
            ResultDescriptor descriptor, Duration resolution) {

        Timestamp upper = end.isPresent() ? end.get() : Timestamp.now();
        Timestamp lower = start.isPresent() ? start.get() : upper.minus(Duration.seconds(86400));

        // Grab and process the samples
        JavaRDD<Row<Sample>> sampleRows = select(resource, Optional.of(lower), Optional.of(upper));
        return new SparkResultProcessor(m_context, resource, lower, upper, descriptor, resolution).process(sampleRows);
    }
}
