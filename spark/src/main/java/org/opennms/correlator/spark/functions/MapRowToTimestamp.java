package org.opennms.correlator.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.persistence.cassandra.SchemaConstants;

import com.datastax.spark.connector.japi.CassandraRow;

public class MapRowToTimestamp implements Function<CassandraRow, Timestamp> {
    private static final long serialVersionUID = -8780119463461242905L;

    @Override
    public Timestamp call(CassandraRow row) throws Exception {
        return Timestamp.fromEpochMillis(row.getDate(SchemaConstants.F_COLLECTED).getTime());
    }
}
