package com.collibra.catalog.tabularprofiler.profilingapiclient;

import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import javax.ws.rs.client.ClientBuilder;

import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.collibra.catalog.profilers.api.ColumnProfilesUpdate;

/**
 * A REST client to send commands to Collibra Catalog.
 */
public final class ProfilingApiClient {
	private final URI rootEndpoint;
	private final String username;
	private final String password;

	public ProfilingApiClient(
			URI rootEndpoint,
			String username,
			String password) {
		this.rootEndpoint = rootEndpoint;
		this.username = username;
		this.password = password;
	}

	public UpdatedColumnProfiles updateColumnProfiles(ColumnProfilesUpdate columnProfiles) {
		return ClientBuilder.newBuilder()
				/*
				 The default connector provider relies on Java's HttpURLConnection, which does not
				 support the PATCH method.
				 One solution is to use the connector provider from Apache's HttpComponents.
				 See:
				 https://jersey.github.io/documentation/latest/client.html#d0e4971
				 https://docs.oracle.com/javase/8/docs/api/java/net/HttpURLConnection.html#setRequestMethod-java.lang.String-
				 */
				.withConfig(new ClientConfig().connectorProvider(new ApacheConnectorProvider()))
				.build()
				.target(rootEndpoint.resolve("/rest/catalog/1.0/profiling/columns"))
				.register(HttpAuthenticationFeature.basic(username, password))
				.request()
				.accept(APPLICATION_JSON_TYPE)
				.build("PATCH", entity(columnProfiles, APPLICATION_JSON_TYPE))
				.invoke(UpdatedColumnProfiles.class);
	}

	public static final class UpdatedColumnProfiles {
		private Long updatedColumnsCount;
		private List<String> errors;

		public Long getUpdatedColumnsCount() {
			return updatedColumnsCount;
		}

		public void setUpdatedColumnsCount(Long updatedColumnsCount) {
			this.updatedColumnsCount = updatedColumnsCount;
		}

		public List<String> getErrors() {
			return errors;
		}

		public void setErrors(List<String> errors) {
			this.errors = errors;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			UpdatedColumnProfiles that = (UpdatedColumnProfiles) o;
			return Objects.equals(updatedColumnsCount, that.updatedColumnsCount) &&
					Objects.equals(errors, that.errors);
		}

		@Override
		public int hashCode() {
			return Objects.hash(updatedColumnsCount, errors);
		}

		@Override
		public String toString() {
			return "UpdatedColumnProfiles{" +
					"updatedColumnsCount=" + updatedColumnsCount +
					", errors=" + errors +
					'}';
		}
	}
}
