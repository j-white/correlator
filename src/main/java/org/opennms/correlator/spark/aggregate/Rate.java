package org.opennms.correlator.spark.aggregate;

import static org.opennms.newts.api.MetricType.GAUGE;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.opennms.newts.api.Gauge;
import org.opennms.newts.api.MetricType;
import org.opennms.newts.api.Sample;
import org.opennms.newts.api.ValueType;
import org.opennms.newts.api.Results.Row;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class Rate {

    public static JavaRDD<Row<Sample>> rate(JavaRDD<Row<Sample>> samples, Set<String> metrics) {
        // Tag all of the samples with their index
        JavaPairRDD<Row<Sample>, Long> zippedSamples = samples.zipWithIndex();
        // Duplicate the samples in the adjacent index and flip the key and values in the pair
        JavaPairRDD<Long, Row<Sample>> sampleWithNeighborByIndex = zippedSamples.flatMapToPair(new FlipCopyAndSubstract());
        // Reduce
        return sampleWithNeighborByIndex.reduceByKey(new RateReducer(metrics)).sortByKey().values();
    }

    public static class RateReducer implements Function2<Row<Sample>, Row<Sample>, Row<Sample>> {
        private static final long serialVersionUID = -7782492450378997477L;

        private static final Gauge NAN = new Gauge(Double.NaN);
        private static final EnumSet<MetricType> COUNTERS = EnumSet.of(MetricType.COUNTER, MetricType.ABSOLUTE, MetricType.DERIVE);

        private final Set<String> m_metrics;

        public RateReducer(Set<String> metrics) {
            m_metrics = metrics;
        }

        @Override
        public Row<Sample> call(Row<Sample> v1, Row<Sample> v2)
                throws Exception {
            // Which one is the actual, and which one is the dup?
            Row<Sample> previous = v1;
            Row<Sample> current = v2;
            Row<Sample> result = new Row<>(current.getTimestamp(), current.getResource());

            for (String metricName : m_metrics) {
                Sample currentSample = current.getElement(metricName);
                Sample previousSample = previous.getElement(metricName);

                if (currentSample == null || previousSample == null) {
                    continue;
                }

                // Use rate as result if one of counter types, else pass through as-is.
                result.addElement(COUNTERS.contains(currentSample.getType()) ? getRate(currentSample, previousSample) : currentSample);
            }

            return result;
        }

        private Sample getRate(Sample currentSample, Sample previousSample) {
            ValueType<?> value = NAN;

            long elapsed = currentSample.getTimestamp().asSeconds() - previousSample.getTimestamp().asSeconds();
            value = currentSample.getValue().delta(previousSample.getValue()).divideBy(elapsed);

            return new Sample(currentSample.getTimestamp(), currentSample.getResource(), currentSample.getName(), GAUGE, value, currentSample.getAttributes());
        }
    }

    public static class FlipCopyAndSubstract implements PairFlatMapFunction<Tuple2<Row<Sample>, Long>, Long, Row<Sample>> {
        private static final long serialVersionUID = -4886245306233548389L;
        @Override
        public Iterable<Tuple2<Long, Row<Sample>>> call(Tuple2<Row<Sample>, Long> t) throws Exception {
            Long index = t._2;
            List<Tuple2<Long, Row<Sample>>> rows = Lists.newArrayListWithCapacity(2);
            // Don't add two entries in the [-1] slot
            if (index > 1) {
                rows.add(new Tuple2<Long, Row<Sample>>(index - 1, t._1));
            }
            rows.add(new Tuple2<Long, Row<Sample>>(index, t._1));
            return rows;
        }
    }
}
