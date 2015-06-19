package org.opennms.correlator.spark;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
import org.opennms.newts.api.ValueType;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.opennms.newts.persistence.cassandra.SchemaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import org.apache.spark.mllib.stat.Statistics;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SparkMetricCorrelator implements MetricCorrelator {

    private static final Logger LOG = LoggerFactory.getLogger(SparkMetricCorrelator.class);

    private static final String APP_NAME = "Correlator";

    private static final String TARGET_JAR = new File("target/correlator-1.0.0-SNAPSHOT.jar").getAbsolutePath();

    private final String keyspace;
    private final Duration resourceShard = Duration.seconds(600000);

    JavaSparkContext sc;
    SparkContextJavaFunctions csc;

    public SparkMetricCorrelator(String sparkMasterUrl, String keyspace) {
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
        sc = new JavaSparkContext(conf);
        csc = CassandraJavaUtil.javaFunctions(sc);
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

    public static class CassandraRowsToSampleRows implements Function<Tuple2<Timestamp, Iterable<CassandraRow>>, Row<Sample>> {
        private static final long serialVersionUID = -2668517612686576960L;
        private final Resource m_resource;

        public CassandraRowsToSampleRows(Resource resource) {
            m_resource = resource;
        }

        @Override
        public Row<Sample> call(Tuple2<Timestamp, Iterable<CassandraRow>> value) throws Exception {
            Row<Sample> sampleRow = new Row<Sample>(value._1, m_resource);
            for (CassandraRow cassandraRow : value._2) {
                sampleRow.addElement(getSample(cassandraRow));
            }
            return sampleRow;
        }

        private Sample getSample(CassandraRow row) {
            ValueType<?> value = getValue(row);
            return new Sample(getTimestamp(row), m_resource, getMetricName(row), value.getType(), value, getAttributes(row));
        }

        private static ValueType<?> getValue(CassandraRow row) {
            return ValueType.compose(row.getBytes(SchemaConstants.F_VALUE));
        }

        private static String getMetricName(CassandraRow row) {
            return row.getString(SchemaConstants.F_METRIC_NAME);
        }

        private static Timestamp getTimestamp(CassandraRow row) {
            return Timestamp.fromEpochMillis(row.getDate(SchemaConstants.F_COLLECTED).getTime());
        }

        private static Map<String, String> getAttributes(CassandraRow row) {
            return row.getMap(SchemaConstants.F_ATTRIBUTES).entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey().toString(),
                                e -> e.getValue().toString()));
        }
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
        return sc.parallelizeDoubles(doubles);
    }

	public Results<Measurement> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end, ResultDescriptor descriptor, Duration resolution) {
	    // Select the the appropriate rows from the database
	    JavaRDD<CassandraRow> rows = select(resource, start, end);
	    // Group the rows by timestamp
	    JavaPairRDD<Timestamp, Iterable<CassandraRow>> rowsKeyedByTimestamp = rows.groupBy(new TimestampExtractor());
	    // Sort the timestamps in ascending order
	    rowsKeyedByTimestamp = rowsKeyedByTimestamp.sortByKey();
	    // Convert the grouped rows into sample rows
	    JavaRDD<Row<Sample>> sampleRows = rowsKeyedByTimestamp.map(new CassandraRowsToSampleRows(resource));

	    // FIXME: No more RDD :(
	    // Calculate the measurements
	    return new ResultProcessor(resource, start.get(), end.get(), descriptor, resolution).process(sampleRows.collect().iterator());
	}

	public JavaRDD<CassandraRow> select(Resource resource, Optional<Timestamp> start, Optional<Timestamp> end) {
        Timestamp lower = start.get().stepFloor(resourceShard);
        Timestamp upper = end.get().stepFloor(resourceShard);

        CassandraJavaRDD<CassandraRow> samples = csc.cassandraTable(keyspace, SchemaConstants.T_SAMPLES);
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
