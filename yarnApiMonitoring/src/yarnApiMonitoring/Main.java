package yarnApiMonitoring;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class Main {

	public static void main(String[] args) {
		String YARN_URL = args[0];
		String postgres_host = args[1];
		String postgres_user = args[2];
		String postgres_password = args[3];
		String cluster_name = args[4];
		try {
			Class.forName("org.postgresql.Driver");
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet("http://" + YARN_URL + "/ws/v1/cluster/apps");
			httpGet.addHeader("Accept", "application/json");
			HttpResponse httpResponse = httpClient.execute(httpGet);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			JsonNode jsonNode = mapper.readTree(responseString);
			ArrayNode jsonArray = (ArrayNode) jsonNode.at("/apps/app");
			Iterator<JsonNode> appsIterator = jsonArray.elements();
			JsonNode jsonNodeCurrent = null;
			
			Connection connection = DriverManager.getConnection("jdbc:postgresql://" + postgres_host + ":5432/test", postgres_user, postgres_password);
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS yarn_apps_monitoring ( " +
					"c_application_id text, c_state text, c_user text, name text, queue text, progress double precision, " +
					"applicationtype text, startedtime bigint, allocatedmb bigint, allocatedvcores bigint, runningcontainers bigint, " +
					"queueusagepercentage double precision, clusterusagepercentage double precision, c_session BIGINT DEFAULT 0, c_cluster TEXT DEFAULT '" + cluster_name + "',"
					+ "c_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp)");
			ResultSet resultSet = statement.executeQuery("SELECT COALESCE(MAX(c_session), 0) + 1 as c_max_session FROM yarn_apps_monitoring");
			
			long c_session = 0;
			if (resultSet.next()) {
				c_session = resultSet.getLong("c_max_session");
			}
			resultSet.close();
			
			while (appsIterator.hasNext()) {
				jsonNodeCurrent = appsIterator.next();
				if (jsonNodeCurrent.get("state").asText().equals("FINISHED")) {
					//System.out.println(jsonNodeCurrent.get("id") + " FINISHED");
				} else if (jsonNodeCurrent.get("state").asText().equals("FAILED")) {
					
				} else if (jsonNodeCurrent.get("state").asText().equals("KILLED")) {
					
				} else {
					System.out.println(jsonNodeCurrent.get("id") + " " + jsonNodeCurrent.get("state"));
					arrayListApps.add(jsonNodeCurrent.get("id").asText());
				}
			}
			String sql = null;
			for (String apps : arrayListApps) {
				httpGet = new HttpGet("http://" + YARN_URL + "/ws/v1/cluster/apps/" + apps);
				httpGet.addHeader("Accept", "application/json");
				httpResponse = httpClient.execute(httpGet);
				responseEntity = httpResponse.getEntity();
				responseString = EntityUtils.toString(responseEntity, "UTF-8");
				jsonNode = mapper.readTree(responseString);
				jsonNodeCurrent = jsonNode.at("/app");
				sql = "INSERT INTO yarn_apps_monitoring VALUES('" + jsonNodeCurrent.get("id").asText() + "', '" +
				jsonNodeCurrent.get("state") + "', '" + jsonNodeCurrent.get("user").asText() + "', '" + jsonNodeCurrent.get("name").asText() + "', '" +
				jsonNodeCurrent.get("queue").asText() + "', " + jsonNodeCurrent.get("progress").asDouble() + ", '" +
				jsonNodeCurrent.get("applicationType").asText() + "', " + jsonNodeCurrent.get("startedTime").asLong() + ", " +
				jsonNodeCurrent.get("allocatedMB").asLong() + ", " + jsonNodeCurrent.get("allocatedVCores").asLong() + ", " +
				jsonNodeCurrent.get("runningContainers").asLong() + ", " + c_session + ")";
				//System.out.println(sql);
				int result = statement.executeUpdate(sql);
			}
			statement.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(1);
		}
	}
	
	final static ObjectMapper mapper = new ObjectMapper();
	static ArrayList<String> arrayListApps = new ArrayList<>();
	
}
