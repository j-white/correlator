package org.opennms.correlator.spark.aggregate;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.opennms.newts.aggregate.IntervalGenerator;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.ValueType;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.query.Datasource;
import org.opennms.newts.api.query.ResultDescriptor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.Tuple2;

/**
 * -Accumulate between [last ts, current ts)
 * 
 * Calculates delta between consecutive timestamps
 *  
 * 
 * @author jesse
 *
 */
public class PrimaryData {

    private static class Accumulation {
        private long m_known, m_unknown;
        private ValueType<?> m_value;
        private Map<String, String> m_attributes = Maps.newHashMap();

        private Accumulation() {
            reset();
        }

        private Accumulation accumulateValue(Duration elapsed, Duration heartbeat, ValueType<?> value) {
            if (elapsed.lt(heartbeat)) {
                m_known += elapsed.asMillis();
                m_value = m_value.plus(value.times(elapsed.asMillis()));
            }
            else {
                m_unknown += elapsed.asMillis();
            }
            return this;
        }

        private Accumulation accumlateAttrs(Map<String, String> attributes) {
            if (attributes != null) m_attributes.putAll(attributes);
            return this;
        }

        private Double getAverage() {
            return isValid() ? m_value.divideBy(m_known).doubleValue() : Double.NaN;
        }

        private long getKnown() {
            return m_known;
        }

        private long getUnknown() {
            return m_unknown;
        }

        private double getElapsed() {
            return getKnown() + getUnknown();
        }

        private boolean isValid() {
            return getUnknown() < (getElapsed() / 2);
        }

        private void reset() {
            m_known = m_unknown = 0;
            m_value = ValueType.compose(0, MetricType.GAUGE);
            m_attributes = Maps.newHashMap();
        }

        private Map<String, String> getAttributes() {
            return m_attributes;
        }

    }
    
    public static class MapRowToNeighboringIntervals implements PairFlatMapFunction<Row<Sample>, Timestamp, Tuple2<Timestamp, Row<Sample>>> {
        private static final long serialVersionUID = 3645855694043360899L;

        private final Timestamp m_start;
        private final Timestamp m_end;
        private final Duration m_interval;
        private final ResultDescriptor m_descriptor;
        private Duration m_maxHeartbeat;

        public MapRowToNeighboringIntervals(Timestamp start, Timestamp end, Duration interval, ResultDescriptor descriptor) {
            m_start = start;
            m_end = end;
            m_interval = interval;
            m_descriptor = descriptor;

            // Determine the max heartbeat
            for (Datasource ds : m_descriptor.getDatasources().values()) {
                Duration heartBeat = ds.getHeartbeat();
                if (m_maxHeartbeat == null) {
                    m_maxHeartbeat = heartBeat;
                } else {
                    if (heartBeat.gt(m_maxHeartbeat)) {
                        m_maxHeartbeat = heartBeat;
                    }
                }
            } 
        }

        @Override
        public Iterable<Tuple2<Timestamp, Tuple2<Timestamp, Row<Sample>>>> call(Row<Sample> row) throws Exception {            
            Timestamp rowTimestamp = row.getTimestamp();
            List<Timestamp> timestamps = Lists.newArrayList();

            Timestamp minInterval = rowTimestamp.minus(m_maxHeartbeat);
            minInterval = minInterval.asMillis() >= 0 ? minInterval : Timestamp.fromEpochMillis(0);
            Timestamp maxInterval = rowTimestamp.plus(m_maxHeartbeat);
 
            IntervalGenerator intervalGenerator = new IntervalGenerator(m_start.stepFloor(m_interval), m_end.stepCeiling(m_interval), m_interval);
            for (Iterator<Timestamp> it = intervalGenerator.iterator(); it.hasNext();){
                Timestamp intervalCeiling = it.next();
                
                if (intervalCeiling.gte(minInterval) && intervalCeiling.lte(maxInterval)) {
                    timestamps.add(intervalCeiling);
                }

                if (maxInterval.lt(intervalCeiling)) {
                    break;
                }
            }

            List<Tuple2<Timestamp, Tuple2<Timestamp, Row<Sample>>>> rows = Lists.newArrayListWithExpectedSize(2);
            for (Timestamp ts : timestamps) {
                if (ts == null) { continue; }
                //System.err.println(String.format("Adding row with ts: %d to %d", rowTs.asSeconds(), ts.asSeconds()));
                rows.add(new Tuple2<Timestamp, Tuple2<Timestamp, Row<Sample>>>(ts, new Tuple2<Timestamp, Row<Sample>>(ts, row)));
            }

            return rows;
        }
    }

    public static class AggregateSamples implements Function<Iterable<Tuple2<Timestamp, Row<Sample>>>, Row<Measurement>> {
        private static final long serialVersionUID = 9213319510747530139L;

        private final Resource m_resource;
        private final ResultDescriptor m_descriptor;

        public AggregateSamples(Resource resource, ResultDescriptor descriptor) {
            m_resource = resource;
            m_descriptor = descriptor;
        }

        @Override
        public Row<Measurement> call(Iterable<Tuple2<Timestamp, Row<Sample>>> rowsWithInterval) throws Exception {
            Duration interval = m_descriptor.getInterval();

            // Grab the timestamp from the first tuple
            // All of the others tuples in the set should have the same timestamp
            Timestamp intervalCeiling = rowsWithInterval.iterator().next()._1;
            Timestamp intervalFloor = intervalCeiling.minus(interval);
            Row<Measurement> output = new Row<Measurement>(intervalCeiling, m_resource);

            // Extract the rows from the tuples and add them to a list
            List<Row<Sample>> rows = StreamSupport.stream(rowsWithInterval.spliterator(), false)
                .filter(tuple -> tuple._2 != null)
                .map(tuple -> tuple._2)
                .collect(Collectors.toList());

            // Sort the list of rows starting with the lowest timestamp
            rows.sort(new Comparator<Row<Sample>>() {
                @Override
                public int compare(Row<Sample> r1, Row<Sample> r2) {
                    return r1.getTimestamp().compareTo(r2.getTimestamp());
                }
            });

            /*
            System.err.println(String.format("T: %d Available samples:", output.getTimestamp().asSeconds()));
            for (Row<Sample> row : rows) {
                Sample s = row.getElements().iterator().next();
                System.err.println(String.format("  T: %d Value: %s", s.getTimestamp().asSeconds(), s.getValue().doubleValue()));
            }
            */

            for (Datasource ds : m_descriptor.getDatasources().values()) {
                // Gather the list of relevant samples
                List<Sample> samples = Lists.newLinkedList();
                for (Row<Sample> sampleRow : rows) {
                    Sample sample = sampleRow.getElement(ds.getSource());
                    // Skip row with missing samples
                    if (sample == null) {
                        continue;
                    }
                    // Skip samples before <= the floor
                    if (sample.getTimestamp().lte(intervalFloor)) {
                        continue;
                    }
                    samples.add(sample);
                }

                // Calculate durations
                Accumulation accumulation = new Accumulation();
                Sample lastSample = null;
                for (Sample sample : samples) {
                    Duration elapsed = null;
                    if (lastSample == null) {
                        if (sample.getTimestamp().lte(intervalCeiling)) {
                            elapsed = sample.getTimestamp().minus(intervalFloor);
                        } else {
                            elapsed = interval.plus(sample.getTimestamp().minus(intervalCeiling));
                        }
                    } else {
                        if (lastSample.getTimestamp().gte(intervalCeiling)) {
                            continue;
                        } else {
                            if (sample.getTimestamp().lte(intervalCeiling)) {
                                elapsed = sample.getTimestamp().minus(lastSample.getTimestamp());
                            } else {
                                elapsed = intervalCeiling.minus(lastSample.getTimestamp());
                            }
                        }
                    }

                    accumulation.accumulateValue(elapsed, ds.getHeartbeat(), sample.getValue());
                    accumulation.accumlateAttrs(sample.getAttributes());
                    
                    lastSample = sample;
                }

                output.addElement(new Measurement(
                        output.getTimestamp(),
                        output.getResource(),
                        ds.getSource(),
                        accumulation.getAverage(),
                        accumulation.getAttributes()));
            }

            return output;
        }
    }

    public static class MapRowToTimestamp implements Function<Row<Measurement>, Timestamp> {
        private static final long serialVersionUID = 7041364812231226030L;

        @Override
        public Timestamp call(Row<Measurement> row) throws Exception {
            return row.getTimestamp();
        }
    }

    public static JavaRDD<Row<Measurement>> primaryData(JavaSparkContext context, JavaRDD<Row<Sample>> samples, Resource resource, Timestamp start, Timestamp end, ResultDescriptor resultDescriptor) {
        // Pair up the samples with the neighboring intervals
        // Samples may belong to multiple intervals
        Duration interval = resultDescriptor.getInterval();
        JavaPairRDD<Timestamp, Tuple2<Timestamp, Row<Sample>>> samplesByTimestamp = samples.flatMapToPair(new MapRowToNeighboringIntervals(start, end, interval, resultDescriptor));

        // Make sure we have at least one row for every interval
        List<Tuple2<Timestamp, Tuple2<Timestamp, Row<Sample>>>> generatedTimestamps = Lists.newLinkedList();
        for (Timestamp ts: new IntervalGenerator(start.stepFloor(interval), end.stepCeiling(interval), interval)) {
            Tuple2<Timestamp, Tuple2<Timestamp, Row<Sample>>> entry = new Tuple2<>(ts, new Tuple2<Timestamp, Row<Sample>>(ts, null));
            generatedTimestamps.add(entry);
        }
        JavaPairRDD<Timestamp, Tuple2<Timestamp, Row<Sample>>> allIntervals = context.parallelizePairs(generatedTimestamps);
        samplesByTimestamp = samplesByTimestamp.union(allIntervals);

        // Group the results by interval and aggregate the resulting set of samples
        return samplesByTimestamp.groupByKey().values().map(new AggregateSamples(resource, resultDescriptor)).sortBy(new MapRowToTimestamp(), true, 0);
    }
}
