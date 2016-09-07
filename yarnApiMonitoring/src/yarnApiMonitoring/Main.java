package yarnApiMonitoring;

import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
		int i = 0;
		String YARN_URL = args[i++];
		String postgres_host = args[i++];
		String postgres_user = args[i++];
		String postgres_password = args[i++];
		String cluster_name = args[i++];
		String writeFile = args[i++];
		try {
			long c_session = 0;
			Statement statement = null;
			Connection connection = null;
			Statement statement_derby = null;
			Connection connection_derby = null;
			Path path = null;
			String dirString = "metrics/yarn/" + simpleDateFormatDate.format(new Date(Calendar.getInstance().getTimeInMillis()));
			Path dir = Paths.get(dirString);
			BufferedWriter bufferedWriter = null;
			if (writeFile != null && (writeFile.equals("y") || writeFile.equals("yes"))) {
				Files.createDirectories(dir);
				path = Paths.get(dirString + "/" + cluster_name + "-yarn-" + simpleDateFormat.format(new Date(Calendar.getInstance().getTimeInMillis())) + ".json");
				bufferedWriter = Files.newBufferedWriter(path, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				String driver = "org.apache.derby.jdbc.EmbeddedDriver";
				Class.forName(driver).newInstance();
				String protocol = "jdbc:derby:";
				connection_derby = DriverManager.getConnection(protocol + "derbyDB;create=true");
				statement_derby = connection_derby.createStatement();
				try {
					statement_derby.executeUpdate("CREATE TABLE application(c_applicationid VARCHAR(150), c_status SMALLINT, c_timestamp TIMESTAMP default current_timestamp)");
				} catch (SQLException e) {
					if (tableAlreadyExists(e)) {

					} else {
						throw e;
					}
				}
				
			} else {
				Class.forName("org.postgresql.Driver");
				connection = DriverManager.getConnection("jdbc:postgresql://" + postgres_host + ":5432/test", postgres_user, postgres_password);
				statement = connection.createStatement();
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS yarn_apps_monitoring ( " +
						"c_application_id text, c_state text, c_user text, name text, queue text, progress double precision, " +
						"applicationtype text, startedtime bigint, allocatedmb bigint, allocatedvcores bigint, runningcontainers bigint, " +
						//"queueusagepercentage double precision, clusterusagepercentage double precision, "
						"c_session BIGINT DEFAULT 0, c_cluster TEXT DEFAULT '" + cluster_name + "',"
						+ "c_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp)");
				ResultSet resultSet = statement.executeQuery("SELECT COALESCE(MAX(c_session), 0) + 1 as c_max_session FROM yarn_apps_monitoring");
				if (resultSet.next()) {
					c_session = resultSet.getLong("c_max_session");
				}
				resultSet.close();
			}
			
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet("http://" + YARN_URL + "/ws/v1/cluster/apps");
			httpGet.addHeader("Accept", "application/json");
			HttpResponse httpResponse = httpClient.execute(httpGet);
			HttpEntity responseEntity = httpResponse.getEntity();
			String responseString = EntityUtils.toString(responseEntity, "UTF-8");
			JsonNode jsonNode = mapper.readTree(responseString);
			//System.out.println(responseString);
			ArrayNode jsonArray = (ArrayNode) jsonNode.at("/apps/app");
			Iterator<JsonNode> appsIterator = jsonArray.elements();
			JsonNode jsonNodeCurrent = null;
			ArrayNode jsonArrayOutput = mapper.createArrayNode();
			String sql = null;
			
			while (appsIterator.hasNext()) {
				jsonNodeCurrent = appsIterator.next();
				if (jsonNodeCurrent.get("state").asText().equals("FINISHED") || jsonNodeCurrent.get("state").asText().equals("FAILED") || (jsonNodeCurrent.get("state").asText().equals("KILLED"))) {
					if (writeFile != null && writeFile.equals("y") || writeFile.equals("yes")) {
						ResultSet resultSet_derby = statement_derby.executeQuery("SELECT 1 FROM application WHERE c_applicationid = '" + jsonNodeCurrent.get("id").asText() + "'");
						if (resultSet_derby.next()) {
							jsonArrayOutput.add(jsonNodeCurrent);
							//System.out.println(jsonArrayOutput.size());
							statement_derby.executeUpdate("DELETE FROM application WHERE c_applicationid = '" + jsonNodeCurrent.get("id").asText() + "'");
						}
						resultSet_derby.close();
					} else {
						sql = "SELECT 1 FROM yarn_apps_monitoring WHERE c_application_id = '" + jsonNodeCurrent.get("id").asText() + "' AND c_state IN ('FINISHED', 'FAILED', 'KILLED')";
						ResultSet resultSet = statement.executeQuery(sql);
						if (resultSet.next()) {
							
						} else {
							sql = "INSERT INTO yarn_apps_monitoring VALUES('" + jsonNodeCurrent.get("id").asText() + "', '" +
									jsonNodeCurrent.get("state") + "', '" + jsonNodeCurrent.get("user").asText() + "', '" + jsonNodeCurrent.get("name").asText() + "', '" +
									jsonNodeCurrent.get("queue").asText() + "', " + jsonNodeCurrent.get("progress").asDouble() + ", '" +
									jsonNodeCurrent.get("applicationType").asText() + "', " + jsonNodeCurrent.get("startedTime").asLong() + ", " +
									jsonNodeCurrent.get("allocatedMB").asLong() + ", " + jsonNodeCurrent.get("allocatedVCores").asLong() + ", " +
									jsonNodeCurrent.get("runningContainers").asLong() + ", " + c_session + ")";
							//System.out.println(sql);
							statement.executeUpdate(sql);
						}
					}
				} else {
					//System.out.println(jsonNodeCurrent);
					if (writeFile != null && writeFile.equals("y") || writeFile.equals("yes")) {
						jsonArrayOutput.add(jsonNodeCurrent);
						System.out.println(jsonArrayOutput.size());
					} else {
						sql = "INSERT INTO yarn_apps_monitoring VALUES('" + jsonNodeCurrent.get("id").asText() + "', '" +
								jsonNodeCurrent.get("state") + "', '" + jsonNodeCurrent.get("user").asText() + "', '" + jsonNodeCurrent.get("name").asText() + "', '" +
								jsonNodeCurrent.get("queue").asText() + "', " + jsonNodeCurrent.get("progress").asDouble() + ", '" +
								jsonNodeCurrent.get("applicationType").asText() + "', " + jsonNodeCurrent.get("startedTime").asLong() + ", " +
								jsonNodeCurrent.get("allocatedMB").asLong() + ", " + jsonNodeCurrent.get("allocatedVCores").asLong() + ", " +
								jsonNodeCurrent.get("runningContainers").asLong() + ", " + c_session + ")";
						//System.out.println(sql);
						statement.executeUpdate(sql);
					}
				}
			}
			
			if (writeFile != null && writeFile.equals("y") || writeFile.equals("yes")) {
				mapper.writeValue(bufferedWriter, jsonArrayOutput);
			} else {
				
			}
			if (statement_derby != null) {
				statement_derby.close();
			}
			if (connection_derby != null) {
				connection_derby.close();
			}
			if (bufferedWriter != null) {
				//bufferedWriter.flush();
				bufferedWriter.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
    public static boolean tableAlreadyExists(SQLException e) {
        boolean exists;
        if(e.getSQLState().equals("X0Y32")) {
            exists = true;
        } else {
            exists = false;
        }
        return exists;
    }
	
	final static ObjectMapper mapper = new ObjectMapper();
	static ArrayList<String> arrayListApps = new ArrayList<>();
	final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
	final static SimpleDateFormat simpleDateFormatDate = new SimpleDateFormat("yyyy-MM-dd");
	
}
