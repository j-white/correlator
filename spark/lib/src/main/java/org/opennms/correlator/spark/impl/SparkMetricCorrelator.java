package org.opennms.correlator.spark.impl;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricWithCoeff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

/**
 * 
 * import com.typesafe.config.{Config, ConfigFactory}
 * val config = ConfigFactory.parseString("{metric={resource=\"a.b\", metric=\"m\"}, candidates=[{resource=\"a.c\", metric=\"n\"}], from: 0, to: 0, resolution: 600000, topN: 10}")
 *
 * @author jwhite
 */
public class SparkMetricCorrelator {

    private static final Logger LOG = LoggerFactory.getLogger(SparkMetricCorrelator.class);

    public List<Map<String, Object>> correlate(SparkContext sc, Config config) {
        // Pull the keyspace from the context
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        String newtsKeyspace = jsc.getConf().get("org.opennms.correlator.spark.newts.keyspace", "newts");

        // Parse the remaining attributes from the config
        Metric target = new Metric(config.getString("metric.resource"),
                config.getString("metric.metric"));

        List<Metric> candidates = Lists.newArrayList();
        for (Config entry : config.getConfigList("candidates")) {
            candidates.add(new Metric(entry.getString("resource"),
                entry.getString("metric")));
        }

        long from = config.getLong("from");
        long to = config.getLong("to");
        long resolution = config.getLong("resolution");
        int topN = config.getInt("topN");

        LOG.info("Performing correlation with keyspace: {}, target: {}, candidates: {}, from: {}, to: {}, resolution: {} and topN: {}",
                newtsKeyspace, target, candidates, from, to, resolution, topN);

        SparkSampleReader ssr = new SparkSampleReader(jsc, newtsKeyspace);
        SparkCorrelator scor = new SparkCorrelator(ssr);
        Collection<MetricWithCoeff> results = scor.correlate(target, candidates, new Date(from), new Date(to), resolution, topN);

        List<Map<String, Object>> entries = Lists.newLinkedList();
        for (MetricWithCoeff result : results) {
            Map<String, Object> entry = Maps.newHashMap();
            entry.put("resource", result.getMetric().getResource());
            entry.put("metric", result.getMetric().getMetric());
            entry.put("coefficient", result.getCoeff());
            entries.add(entry);
        }
        return entries;
    }
}
