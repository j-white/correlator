package org.opennms.correlator;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Correlator {
	
	public static class FunctionA implements Function<String, Boolean> {
		private static final long serialVersionUID = 29833457434818700L;

		public Boolean call(String s) {
			return s.contains("a");
		}
	}
	
	public static class FunctionB implements Function<String, Boolean> {
		private static final long serialVersionUID = 1858481264346052827L;

		public Boolean call(String s) {
			return s.contains("b");
		}
	}
	
	public long getNumLines(File f) {
		String logFile = f.toURI().toString();
		String targetJar = new File("target/sparker-0.0.1-SNAPSHOT.jar").getAbsolutePath();

		System.out.println("Using log file: "  + logFile);
	    SparkConf conf = new SparkConf().setAppName("Simple Application")
	    		.setMaster("spark://noise:7077")
	    		.setJars(new String[] {
	    				targetJar
	    		});
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();
	
	    long numAs = logData.filter(new FunctionA()).count();
	
	    long numBs = logData.filter(new FunctionB()).count();

	    sc.close();
	    return numAs + numBs;
	}
}
