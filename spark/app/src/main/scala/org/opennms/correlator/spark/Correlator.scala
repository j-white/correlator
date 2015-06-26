package org.opennms.correlator.spark;

import org.opennms.correlator.spark.impl.{SparkMetricCorrelator}
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.{SparkJob, SparkJobValidation, SparkJobValid, SparkJobInvalid}
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.util.Try

/**
 * My Spark job
 */
object Correlator extends SparkJob {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "Correlator")
    val config = ConfigFactory.parseString("metric={resource='a.b', metric='m'}, from: 0, to: 0, resolution: 600000, topN: 10")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("metric.resource"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No metric.resource config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    var smc = new SparkMetricCorrelator()
    smc.correlate(sc, config)
  }
}