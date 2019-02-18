package com.collibra.catalog.tabularprofiler;

import java.net.URI;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.collibra.catalog.profilers.api.AssetIdentifier;
import com.collibra.catalog.profilers.api.ColumnProfilesUpdate;
import com.collibra.catalog.profilers.api.ProfileFeatures;
import com.collibra.catalog.profilers.api.Profilers;
import com.collibra.catalog.tabularprofiler.profilingapiclient.ProfilingApiClient;

public class Csv {
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Csv.class);

	public static void main(String[] args) {
		// initialize logging
		BasicConfigurator.configure();
		// initialize Spark
		SparkSession sparkSession = SparkSession.builder()
				.appName("column-profiler")
				.master("local")
				.getOrCreate();

		// define the data to process. This is the input for the profiling jobs.
		String csvInputPath = Csv.class.getResource("/datasets/iris.csv").getPath();
		log.info("Input file path: {}", csvInputPath);
		Dataset<Row> dataset = sparkSession.read()
				.option("header", true) // read column names from csv header
				.csv(csvInputPath);

		// Execute the profiling jobs and map each column profiles to an asset in Collibra.
		// It's possible to do it in 2 separate steps. See JDBC example.
		ColumnProfilesUpdate profileUpdate = Profilers.profileTable(
				dataset,
				ProfileFeatures.BASIC_STATISTICS,
//				ProfileFeatures.BASIC_STATISTICS_AND_QUANTILES,
//				ProfileFeatures.FULL,
				columnName -> AssetIdentifier.builder()
						.assetName("iris.csv > " + columnName)
						.communityName("Schemas")
						.domainName("Iris")
						.build()
		);
		sparkSession.stop();

		// store the profiles in Collibra
		log.info("Sending column profiles update: \n{}", profileUpdate);
		ProfilingApiClient profilingApiClient = new ProfilingApiClient(
				URI.create("http://replace.with.collibra.url:4400"),
				"SomeUser",
				"SomePassword");
		ProfilingApiClient.UpdatedColumnProfiles response = profilingApiClient.updateColumnProfiles(profileUpdate);
		log.info("Got response:\n{}", response);
	}
}
