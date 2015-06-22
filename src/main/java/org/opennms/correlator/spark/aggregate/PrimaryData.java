package org.opennms.correlator.spark.aggregate;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.opennms.newts.aggregate.IntervalGenerator;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.query.ResultDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class PrimaryData {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryData.class);

    public static class IntervalGrouper implements Function<Row<Sample>, Timestamp> {
        private static final long serialVersionUID = -540408081946546652L;

        private final Timestamp m_start;
        private final Timestamp m_end;
        private final Duration m_interval;
        // This list may be very large, so don't serialize it
        private transient List<Timestamp> m_timestamps;

        public IntervalGrouper(Timestamp start, Timestamp end, Duration interval) {
            m_start = start;
            m_end = end;
            m_interval = interval;
        }

        @Override
        public Timestamp call(Row<Sample> row) throws Exception {
            Timestamp rowTs = row.getTimestamp();
            Iterator<Timestamp> timestamps =  new IntervalGenerator(m_start.stepFloor(m_interval), m_end.stepCeiling(m_interval), m_interval);

            // TODO: What group should the samples actually go into?
            // TODO: What happens if the samples don't actually belong to any group?
            Timestamp currentTs;
            while(timestamps.hasNext()) {
                currentTs = timestamps.next();
                if (rowTs.lte(currentTs)) {
                    return currentTs;
                }
            }

            return null;
        }
    }

    public static class AggregateSeqFunc implements Function2<Row<Measurement>, Iterable<Tuple2<Timestamp, Row<Sample>>>, Row<Measurement>> {
        private static final long serialVersionUID = -8146152113164216035L;
        private Resource m_resource;
        private ResultDescriptor m_descriptor;

        public AggregateSeqFunc(Resource resource, ResultDescriptor descriptor) {
            m_resource = resource;
            m_descriptor = descriptor;
        }

        @Override
        public Row<Measurement> call(Row<Measurement> measurements, Iterable<Tuple2<Timestamp, Row<Sample>>> samples) throws Exception {
            LOG.debug("aggregateSeq called with: {} and samples: {}", measurements, samples);
            for (Tuple2<Timestamp, Row<Sample>> sampleTuple : samples) {
                if (measurements == null) {
                    measurements = new Row<Measurement>(sampleTuple._1, m_resource);
                }
                
                if (sampleTuple._2 != null) {
                    Row<Sample> sampleRow = sampleTuple._2;
                    Sample sample = sampleRow.getElements().iterator().next();
                    measurements.addElement(new Measurement(sampleTuple._1, m_resource, sample.getName(), sample.getValue().doubleValue()));
                } else {
                    for (String source : m_descriptor.getSourceNames()) {
                        measurements.addElement(new Measurement(sampleTuple._1, m_resource, source, Double.NaN));
                    }
                }
            }
            return measurements;
        }
    }

    public static class ComboSeqFunc implements Function2<Row<Measurement>, Row<Measurement>, Row<Measurement>> {
        private static final long serialVersionUID = -4102964274968427023L;

        @Override
        public Row<Measurement> call(Row<Measurement> v1, Row<Measurement> v2) throws Exception {
            LOG.debug("comboSeq called with v1: {} v2: {}", v1, v2);
            return v1 != null ? v1 : v2;
        }
    }

    public static class MapToTupleWithInterval implements Function<Row<Sample>, Tuple2<Timestamp, Row<Sample>>> {
        private static final long serialVersionUID = -2003891029707306817L;

        private final Timestamp m_start;
        private final Timestamp m_end;
        private final Duration m_interval;

        public MapToTupleWithInterval(Timestamp start, Timestamp end, Duration interval) {
            m_start = start;
            m_end = end;
            m_interval = interval;
        }

        @Override
        public Tuple2<Timestamp, Row<Sample>> call(Row<Sample> row) throws Exception {
            Timestamp rowTs = row.getTimestamp();
            Iterator<Timestamp> timestamps =  new IntervalGenerator(m_start.stepFloor(m_interval), m_end.stepCeiling(m_interval), m_interval);

            // TODO: What group should the samples actually go into?
            // TODO: What happens if the samples don't actually belong to any group?
            Timestamp ts = null;
            while(timestamps.hasNext()) {
                ts = timestamps.next();
                if (rowTs.lte(ts)) {
                    break;
                }
            }

            return new Tuple2<Timestamp, Row<Sample>>(ts, row);
        }
    }

    public static class GroupByTimestamp implements Function<Tuple2<Timestamp, Row<Sample>>, Timestamp> {
        private static final long serialVersionUID = -5476269492445222692L;

        @Override
        public Timestamp call(Tuple2<Timestamp, Row<Sample>> val) throws Exception {
            return val._1;
        }
    }

    public static JavaRDD<Row<Measurement>> primaryData(JavaSparkContext context, JavaRDD<Row<Sample>> samples, Resource resource, Timestamp start, Timestamp end, ResultDescriptor resultDescriptor) {
        // Determine the intervals and add them to the row
        Duration interval = resultDescriptor.getInterval();
        JavaRDD<Tuple2<Timestamp, Row<Sample>>> samplesWithInterval = samples.map(new MapToTupleWithInterval(start, end, interval));

         // Group all of the samples by interval
        JavaPairRDD<Timestamp, Iterable<Tuple2<Timestamp, Row<Sample>>>> groupedSamples = samplesWithInterval.groupBy(new GroupByTimestamp());

        // TODO: What happens when there are no rows for a given timestamp
        
        List<Tuple2<Timestamp, Iterable<Tuple2<Timestamp, Row<Sample>>>>> generatedTimestamps = Lists.newLinkedList();
        for (Timestamp ts: new IntervalGenerator(start.stepFloor(interval), end.stepCeiling(interval), interval)) {
            
            @SuppressWarnings("unchecked")
            Iterable<Tuple2<Timestamp, Row<Sample>>> emptyList = Lists.newArrayList(
                        new Tuple2<Timestamp, Row<Sample>>(ts, null)
                    );
            Tuple2<Timestamp, Iterable<Tuple2<Timestamp, Row<Sample>>>> entry = new Tuple2<>(ts, emptyList);
            generatedTimestamps.add(entry);
        }
        JavaPairRDD<Timestamp, Iterable<Tuple2<Timestamp, Row<Sample>>>> allIntervals = context.parallelizePairs(generatedTimestamps);

        groupedSamples = groupedSamples.union(allIntervals);

        // Aggregate
        return groupedSamples.aggregateByKey(null, new AggregateSeqFunc(resource, resultDescriptor), new ComboSeqFunc()).sortByKey().values();
    }
}
