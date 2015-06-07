package org.opennms.correlator.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkLineCounter {
	
	public static class CountLinesContaining implements Function<String, Boolean> {
		private static final long serialVersionUID = 29833457434818700L;

		private final String m_substring;
		
		public CountLinesContaining(String substring) {
			m_substring = substring;
		}
		
		public Boolean call(String s) {
			return s.contains(m_substring);
		}
	}
	
	public long getNumLines(File f) {
		String logFile = f.toURI().toString();
		String targetJar = new File("target/correlator-1.0.0-SNAPSHOT.jar").getAbsolutePath();

		System.out.println("Using log file: "  + logFile);
	    SparkConf conf = new SparkConf().setAppName("Correlator")
	    		.setMaster("spark://noise:7077")
	    		.setJars(new String[] {
	    				targetJar
	    		});
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();
	
	    long numAs = logData.filter(new CountLinesContaining("a")).count();
	
	    long numBs = logData.filter(new CountLinesContaining("b")).count();

	    sc.close();
	    return numAs + numBs;
	}
}
