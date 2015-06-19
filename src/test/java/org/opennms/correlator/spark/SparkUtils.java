package org.opennms.correlator.spark;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.ValueType;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.persistence.cassandra.SchemaConstants;

import scala.Tuple2;

import com.datastax.spark.connector.japi.CassandraRow;

public class SparkUtils {

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

}
