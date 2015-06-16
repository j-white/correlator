package org.opennms.correlator.spark;

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.opennms.correlator.api.Metric;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.base.Optional;

public class SparkLineCounter {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLineCounter.class);

    private static final String APP_NAME = "Correlator";

    private static final String TARGET_JAR = new File("target/correlator-1.0.0-SNAPSHOT.jar").getAbsolutePath();

    private final String keyspace;
    private final Duration resourceShard = Duration.seconds(600000);

    JavaSparkContext sc;
    SparkContextJavaFunctions csc;

    public SparkLineCounter(String sparkMasterUrl, String keyspace) {
        this.keyspace = keyspace;

        SparkConf conf = new SparkConf().setAppName(APP_NAME)
                .setMaster(sparkMasterUrl)
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.port", "9142")
                .setJars(new String[] {
                        TARGET_JAR,
                        "/home/jesse/.m2/repository/com/datastax/spark/spark-cassandra-connector_2.10/1.4.0-M1/spark-cassandra-connector_2.10-1.4.0-M1.jar",
                        "/home/jesse/.m2/repository/com/datastax/spark/spark-cassandra-connector-java_2.10/1.4.0-M1/spark-cassandra-connector-java_2.10-1.4.0-M1.jar",
                        "/home/jesse/.m2/repository/com/datastax/cassandra/cassandra-driver-core/2.1.5/cassandra-driver-core-2.1.5.jar",
                        "/home/jesse/.m2/repository/com/google/guava/guava/18.0/guava-18.0.jar",
                        "/home/jesse/.m2/repository/joda-time/joda-time/2.3/joda-time-2.3.jar",
                        "/home/jesse/.m2/repository/org/opennms/newts/newts-api/1.2.1-SNAPSHOT/newts-api-1.2.1-SNAPSHOT.jar"
                });
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkContextJavaFunctions csc = CassandraJavaUtil.javaFunctions(sc);
    }

	public static class CountLinesContaining implements Function<String, Boolean> {
		private static final long serialVersionUID = 29833457434818700L;

		private final String m_substring;

		public CountLinesContaining(String substring) {
			m_substring = substring;
		}

		public Boolean call(String s) {
			return s.contains(m_substring);
		}
	}

	public static class MetricFilter implements Function<CassandraRow, Boolean> {
        private static final long serialVersionUID = -6366270126240654148L;
        private final Metric metric;

        public MetricFilter(Metric metric) {
            this.metric = metric;
        }

        @Override
        public Boolean call(CassandraRow row) throws Exception {
            return row.getString(SchemaConstants.F_METRIC_NAME) != metric.getMetric();
        }
    }

	public static class TimestampExtractor implements Function<CassandraRow, Timestamp> {
        private static final long serialVersionUID = -8780119463461242905L;

        @Override
        public Timestamp call(CassandraRow row) throws Exception {
            return Timestamp.fromEpochMillis(row.getDate(SchemaConstants.F_COLLECTED).getTime());
        }
	}

	public static class RowsToSamplesSeq implements Function2<Results.Row<Sample>, Iterable<CassandraRow>, Results.Row<Sample>> {
        private static final long serialVersionUID = -7760420110853348556L;

        @Override
        public Row<Sample> call(Row<Sample> v1, Iterable<CassandraRow> v2) throws Exception {
            return null;
        }
	};

    public static class RowsToSamplesCombo implements Function2<Results.Row<Sample>, Results.Row<Sample>, Results.Row<Sample>> {
        private static final long serialVersionUID = -3544815565399178843L;

        @Override
        public Row<Sample> call(Row<Sample> v1, Row<Sample> v2) throws Exception {
            // TODO Auto-generated method stub
            return null;
        }
    };

	public Results<Measurement> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end, ResultDescriptor descriptor, Duration resolution) {
	    JavaRDD<CassandraRow> rows = select(resource, start, end);
	    JavaPairRDD<Timestamp, Iterable<CassandraRow>> rowsByTimestamp = rows.groupBy(new TimestampExtractor());
	    JavaPairRDD<Timestamp, Results.Row<Sample>> sampleRowsByTimestamp = rowsByTimestamp.aggregateByKey(null, new RowsToSamplesSeq(), new RowsToSamplesCombo());

	    // FIXME: No more RDD :(
	    List<Row<Sample>> sampleRows = sampleRowsByTimestamp.values().collect();
	    return new ResultProcessor(resource, start.get(), end.get(), descriptor, resolution).process(sampleRows.iterator());
	}

	public JavaRDD<CassandraRow> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end) {
        Timestamp lower = start.get().stepFloor(resourceShard);
        Timestamp upper = end.get().stepFloor(resourceShard);

        CassandraJavaRDD<CassandraRow> samples = csc.cassandraTable(keyspace, SchemaConstants.T_SAMPLES);
        JavaRDD<CassandraRow> rows = null;
        for (Timestamp partition : new IntervalGenerator(lower, upper, resourceShard)) {
            JavaRDD<CassandraRow> nextRows = samples.select(SchemaConstants.F_METRIC_NAME, SchemaConstants.F_VALUE, SchemaConstants.F_ATTRIBUTES)
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
