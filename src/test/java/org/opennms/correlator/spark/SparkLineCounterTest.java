package org.opennms.correlator.spark;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opennms.correlator.spark.SparkLineCounter;

public class SparkLineCounterTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test(timeout=300000)
	public void canGetNumberOfLines() throws IOException {
		String s = "a\nb\n";

		File f = tempFolder.newFile();
		FileUtils.writeStringToFile(f, "a\nb\n");
		assertEquals(s, FileUtils.readFileToString(f));

		String hostname = InetAddress.getLocalHost().getHostName();
		SparkLineCounter sparker = new SparkLineCounter("spark://" + hostname + ":7077");
		assertEquals(2, sparker.getNumLines(f));
	}
}
