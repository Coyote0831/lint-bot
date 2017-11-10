package line.bot.util;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.bot.client.LineMessagingClientBuilder;
import com.linecorp.bot.model.profile.UserProfileResponse;

import line.bot.property.LineProperty;

public class UserProfileCache {

	private static Logger log = LoggerFactory.getLogger(UserProfileCache.class);

	private static HashMap<String, UserProfileResponse> userProfileCache = null;

	static {
		UserProfileCache.userProfileCache = new HashMap<String, UserProfileResponse>();
	}

	public static UserProfileResponse getUserProfile(String groupId, String userId) throws RuntimeException {
		if (UserProfileCache.userProfileCache.containsKey(userId)) {
			return UserProfileCache.userProfileCache.get(userId);
		} else {
			log.info("Profile not exist, start to update cache.");
			UserProfileCache.updateUserProfileCache(groupId, userId);
			return UserProfileCache.userProfileCache.get(userId);
		}
	}

	private static void updateUserProfileCache(String groupId, String userId) throws RuntimeException {
		UserProfileResponse userProfileResponse;
		try {

			LineMessagingClientBuilder lineMessagingClientBuilder = new LineMessagingClientBuilder(
					LineProperty.getChannelAccessToken());

			log.info(String.format("Start to get profile with group id [%s], user id [%s].", groupId, userId));
			userProfileResponse = lineMessagingClientBuilder.build().getGroupMemberProfile(groupId, userId).get();

			lineMessagingClientBuilder.removeAllInterceptors();
			lineMessagingClientBuilder = null;

		} catch (InterruptedException | ExecutionException e) {
			String errorMsg = String.format("Failed to update user profile cache with group id %s and user id %s",
					groupId, userId);
			log.error(errorMsg, e.getMessage());
			throw new RuntimeException(errorMsg);
		}

		UserProfileCache.userProfileCache.put(userId, userProfileResponse);
	}
	
	public static void clearCache() {
		log.info("Start to clear cache.");
		UserProfileCache.userProfileCache.clear();
		log.info("Profile cache cleared.");
	}
}
