package line.bot.operation;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linecorp.bot.model.profile.UserProfileResponse;

import line.bot.pojo.ViewFormat;
import line.bot.util.DbHandler;
import line.bot.util.UserProfileCache;

public class ViewHandler {
	private static Logger log = LoggerFactory.getLogger(ViewHandler.class);

	public static Object getViewData(String format) {

		if (StringUtils.equals(format, ViewFormat.JSON.name())) {
			return ViewHandler.getJsonViewData();
		}

		return ViewHandler.getJsonViewData();
	}

	public static JsonObject getJsonViewData() {
		List<String> tables = DbHandler.listTables();

		JsonObject result = new JsonObject();
		tables.forEach(table -> {

			if (table.startsWith("group")) {

				JsonArray searchResult = DbHandler.getGroupTableDatas(table);

				String groupId = Character.toUpperCase(table.split("_")[1].charAt(0))
						+ table.split("_")[1].substring(1);

				JsonArray buildResult = new JsonArray();

				searchResult.forEach(element -> {

					JsonObject user = element.getAsJsonObject();

					try {
						UserProfileResponse profile = UserProfileCache.getUserProfile(groupId,
								user.get("user_id").getAsString());

						user.addProperty("display_name", profile.getDisplayName());
						user.addProperty("pic_url", profile.getPictureUrl());
					} catch (RuntimeException e) {
						log.info("Failed to get profile, skip and continue next loop.");
					}
					buildResult.add(user);
				});

				result.add(table, buildResult);
			}
		});

		return result;
	}
}
