package org.opennms.correlator.spark;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.opennms.correlator.api.MetricCorrelator;
import org.opennms.correlator.newts.AbstractCorrelatorTest;
import org.opennms.correlator.spark.impl.SparkSampleReader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Throwables;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
    "classpath:/applicationContext-newts.xml"
})
public class SparkMetricCorrelatorTest extends AbstractCorrelatorTest {

    private static final String APP_NAME = "Correlator";

    private static final String TARGET_JAR = new File("target/correlator-1.0.0-SNAPSHOT.jar").getAbsolutePath();

    private SparkSampleReader m_sampleReader;

    private SparkMetricCorrelator m_correlator;

    @Before
    public void setUp() {
        String sparkMasterUrl = "local";
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw Throwables.propagate(e);
        }
        sparkMasterUrl = "spark://" + hostname + ":7077";

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
                        "/home/jesse/.m2/repository/org/opennms/newts/newts-api/1.2.1-SNAPSHOT/newts-api-1.2.1-SNAPSHOT.jar",
                        "/home/jesse/.m2/repository/org/opennms/newts/newts-aggregate/1.2.1-SNAPSHOT/newts-aggregate-1.2.1-SNAPSHOT.jar"
                });

        JavaSparkContext sc = new JavaSparkContext(conf);
        m_sampleReader = new SparkSampleReader(sc, "newts");
        m_correlator = new SparkMetricCorrelator(m_sampleReader);
        super.setUp();
    }

    @Override
    public MetricCorrelator getCorrelator() {
        return m_correlator;
    }
}
