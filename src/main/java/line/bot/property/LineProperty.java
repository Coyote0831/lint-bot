package line.bot.property;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LineProperty {

	private static String channelAccessToken = StringUtils.EMPTY;
	private static int flushDurationMinute = 0;
	private static String dbConnectionUri = StringUtils.EMPTY;
	private static String dbUserName = StringUtils.EMPTY;
	private static String dbPassword = StringUtils.EMPTY;

	private LineProperty() {
	}

	public static synchronized void initiate() throws IOException {

		try (InputStream fileStream = LineProperty.class.getResourceAsStream("/lineProperty.json")) {
			JsonObject lineProperty = new JsonParser()
					.parse(IOUtils.toString(fileStream, StandardCharsets.UTF_8.name())).getAsJsonObject();
			LineProperty.setChannelAccessToken(lineProperty.get("channel_access_token").getAsString());
			LineProperty.setFlushDurationMinute(lineProperty.get("flush_duration_minute").getAsInt());
			LineProperty.setDbConnectionUri(lineProperty.get("db_connection_uri").getAsString());
			LineProperty.setDbUserName(lineProperty.get("db_username").getAsString());
			LineProperty.setDbPassword(lineProperty.get("db_password").getAsString());
		}
	}

	public static String getChannelAccessToken() {
		return channelAccessToken;
	}

	public static void setChannelAccessToken(String channelAccessToken) {
		LineProperty.channelAccessToken = channelAccessToken;
	}

	public static int getFlushDurationMinute() {
		return flushDurationMinute;
	}

	public static void setFlushDurationMinute(int flushDurationMinute) {
		LineProperty.flushDurationMinute = flushDurationMinute;
	}

	public static String getDbConnectionUri() {
		return dbConnectionUri;
	}

	public static void setDbConnectionUri(String dbConnectionUri) {
		LineProperty.dbConnectionUri = dbConnectionUri;
	}

	public static String getDbUserName() {
		return dbUserName;
	}

	public static void setDbUserName(String dbUserName) {
		LineProperty.dbUserName = dbUserName;
	}

	public static String getDbPassword() {
		return dbPassword;
	}

	public static void setDbPassword(String dbPassword) {
		LineProperty.dbPassword = dbPassword;
	}
}
