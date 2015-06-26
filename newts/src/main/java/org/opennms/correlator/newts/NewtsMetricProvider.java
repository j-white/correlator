package org.opennms.correlator.newts;

import java.util.List;

import org.opennms.correlator.api.Metric;
import org.opennms.correlator.api.MetricProvider;
import org.opennms.newts.api.search.QueryBuilder;
import org.opennms.newts.api.search.SearchResults;
import org.opennms.newts.api.search.SearchResults.Result;
import org.opennms.newts.api.search.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class NewtsMetricProvider implements MetricProvider {

    private static final Logger LOG = LoggerFactory.getLogger(NewtsMetricProvider.class);

    private Searcher m_searcher;

    @Override
    public List<Metric> getAllMetrics() {
        return Lists.newArrayList(
                new Metric("snmp:fs:NODES:ny-cassandra-3:mib2-tcp", "tcpActiveOpens"),
                new Metric("snmp:fs:NODES:ny-cassandra-3:mib2-tcp", "tcpCurrEstab"),
                new Metric("snmp:fs:NODES:ny-cassandra-3:ucd-loadavg", "loadavg1"),
                new Metric("snmp:fs:NODES:ny-cassandra-3:ucd-loadavg", "loadavg5"),
                new Metric("snmp:fs:NODES:ny-cassandra-3:ucd-loadavg", "loadavg15")
                );

        /*
        LOG.info("Searching for metrics...");
        SearchResults searchResults = m_searcher.search(QueryBuilder.matchAnyValue("response", "snmp"));
        LOG.info("Found {} metrics", searchResults.size());

        List<Metric> metrics = Lists.newArrayList();
        for (Result result : searchResults) {
            String resourceId = result.getResource().getId();
            for (String metric : result.getMetrics()) {
                metrics.add(new Metric(resourceId, metric));
            }
        }
        return metrics;
        */
    }

    public void setSearcher(Searcher searcher) {
        m_searcher = searcher;
    }

    public Searcher getSearcher() {
        return m_searcher;
    }
}
