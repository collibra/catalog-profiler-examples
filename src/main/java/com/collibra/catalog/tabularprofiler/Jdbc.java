package com.collibra.catalog.tabularprofiler;

import java.net.URI;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.collibra.catalog.profilers.api.AssetIdentifier;
import com.collibra.catalog.profilers.api.ColumnProfilesUpdate;
import com.collibra.catalog.profilers.api.ProfileFeatures;
import com.collibra.catalog.profilers.api.Profilers;
import com.collibra.catalog.tabularprofiler.profilingapiclient.ProfilingApiClient;


public class Jdbc {
	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Jdbc.class);

	public static void main(String[] args) {
		// initialize logging
		BasicConfigurator.configure();
		// initialize Spark
		SparkSession sparkSession = SparkSession.builder()
				.appName("column-profiler")
				.master("local")
				.getOrCreate();

		// define the data to process. This is the input for the profiling jobs.
		Properties jdbcProperties = new Properties();
		jdbcProperties.setProperty("user", "dgc");
		jdbcProperties.setProperty("password", "dgc");
		Dataset<Row> dataset = sparkSession.read().jdbc(
				"jdbc:postgresql://localhost:5432/iris",
				"iris",
				jdbcProperties);

		// Execute the profiling jobs
		ColumnProfilesUpdate profileUpdate = Profilers.profileTable(
				dataset,
				ProfileFeatures.BASIC_STATISTICS
//				ProfileFeatures.BASIC_STATISTICS_AND_QUANTILES
//				ProfileFeatures.FULL
		);
		sparkSession.stop();

		// map each column profiles to an asset in Collibra Catalog.
		// It's possible to do it with the profiling. See CSV example.
		profileUpdate.getColumnProfiles().forEach(
				profile -> profile.setAssetIdentifier(AssetIdentifier.builder()
							.assetName("iris > " + profile.getColumnName())
							.communityName("Schemas")
							.domainName("Iris")
							.build())
		);

		// store the profiles in Collibra Catalog
		log.info("Sending column profiles update: \n{}", profileUpdate);
		ProfilingApiClient profilingApiClient = new ProfilingApiClient(
				URI.create("https://replace.with.collibra.url:4400"),
				"SomeUser",
				"SomePassword");
		ProfilingApiClient.UpdatedColumnProfiles response = profilingApiClient.updateColumnProfiles(profileUpdate);
		log.info("Got response:\n{}", response);
	}
}
