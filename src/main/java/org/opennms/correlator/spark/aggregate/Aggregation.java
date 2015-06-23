package org.opennms.correlator.spark.aggregate;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.opennms.newts.aggregate.IntervalGenerator;
import org.opennms.newts.api.Duration;
import org.opennms.newts.api.Measurement;
import org.opennms.newts.api.Resource;
import org.opennms.newts.api.Timestamp;
import org.opennms.newts.api.Results.Row;
import org.opennms.newts.api.query.Datasource;
import org.opennms.newts.api.query.ResultDescriptor;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import scala.Tuple2;

public class Aggregation {

    public static class MapRowToInterval implements PairFunction<Row<Measurement>, Timestamp, Tuple2<Timestamp, Row<Measurement>>> {
        private static final long serialVersionUID = -1519207769165024666L;

        private final Timestamp m_start;
        private final Timestamp m_end;
        private final Duration m_resolution;

        public MapRowToInterval(Timestamp start, Timestamp end, Duration resolution) {
            m_start = start;
            m_end = end;
            m_resolution = resolution;
        }

        @Override
        public Tuple2<Timestamp, Tuple2<Timestamp, Row<Measurement>>> call(Row<Measurement> row) throws Exception {
            for (Timestamp rangeUpper : new IntervalGenerator(m_start.stepFloor(m_resolution), m_end.stepCeiling(m_resolution), m_resolution)) {
                Timestamp rangeLower = rangeUpper.minus(m_resolution);
                if (row.getTimestamp().lte(rangeUpper) && row.getTimestamp().gt(rangeLower)) {
                    return new Tuple2<Timestamp, Tuple2<Timestamp, Row<Measurement>>>(rangeUpper,
                            new Tuple2<Timestamp, Row<Measurement>>(rangeUpper, row));
                }
            }

            throw new IllegalArgumentException("Row doesn't fall into any interval!");
        }
    }

    public static class Mapper implements Function<Iterable<Tuple2<Timestamp, Row<Measurement>>>, Row<Measurement>> {
        private static final long serialVersionUID = 4122652263727623111L;

        private final Resource m_resource;
        private final Duration m_resolution;
        private final ResultDescriptor m_descriptor;
        private final double m_intervalsPer;

        public Mapper(Resource resource, Duration resolution, ResultDescriptor descriptor) {
            m_resource = resource;
            m_resolution = resolution;
            m_descriptor = descriptor;

            Duration interval = m_descriptor.getInterval();
            m_intervalsPer = (double) m_resolution.divideBy(interval);
        }

        @Override
        public Row<Measurement> call(Iterable<Tuple2<Timestamp, Row<Measurement>>> entries) throws Exception {
            Timestamp targetTs = null;
            List<Row<Measurement>> rows = Lists.newArrayList();

            for (Tuple2<Timestamp, Row<Measurement>> entry : entries) {
                if (targetTs == null) {
                    targetTs = entry._1;
                }
                rows.add(entry._2);
            }
            
            if (targetTs == null) {
                throw new IllegalArgumentException("Need at least one row!");
            }

            Multimap<String, Double> values = ArrayListMultimap.create();
            Map<String, Map<String, String>> aggregatedAttrs = Maps.newHashMap();

            for (Row<Measurement> row : rows) {
                for (Datasource ds : m_descriptor.getDatasources().values()) {
                    Measurement metric = row.getElement(ds.getSource());
                    values.put(ds.getLabel(), metric != null ? metric.getValue() : Double.NaN);

                    Map<String, String> metricAttrs = aggregatedAttrs.get(ds.getLabel());
                    if (metricAttrs == null) {
                        metricAttrs = Maps.newHashMap();
                        aggregatedAttrs.put(ds.getLabel(), metricAttrs);
                    }

                    if (metric.getAttributes() != null) {
                        metricAttrs.putAll(metric.getAttributes());
                    }
                }
            }

            Row<Measurement> target = new Row<Measurement>(targetTs, m_resource);
            for (Datasource ds : m_descriptor.getDatasources().values()) {
                Double v = aggregate(ds, values.get(ds.getLabel()));
                Map<String, String> attrs = aggregatedAttrs.get(ds.getLabel());
                target.addElement(new Measurement(target.getTimestamp(), m_resource, ds.getLabel(), v, attrs));
            }

            return target;
        }

        private Double aggregate(Datasource ds, Collection<Double> values) {
            return ((values.size() / m_intervalsPer) > ds.getXff()) ? ds.getAggregationFuction().apply(values) : Double.NaN;
        }
    }

    public static JavaRDD<Row<Measurement>> aggregate(JavaSparkContext context, JavaRDD<Row<Measurement>> primaryData, Resource resource, Timestamp start, Timestamp end, ResultDescriptor resultDescriptor, Duration resolution) {
        Duration interval = resultDescriptor.getInterval();
        checkArgument(resolution.isMultiple(interval), "resolution must be a multiple of interval");

        JavaPairRDD<Timestamp, Tuple2<Timestamp, Row<Measurement>>> rowsByInterval = primaryData.mapToPair(new MapRowToInterval(start, end, resolution));
        return rowsByInterval.sortByKey().groupByKey().values().map(new Mapper(resource, resolution, resultDescriptor));
    }
}
