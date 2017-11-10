package line.bot.util;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.bot.model.event.MessageEvent;

public class LatestMessageCache {

	private static Logger log = LoggerFactory.getLogger(LatestMessageCache.class);

	/***
	 * Key of the map is a prefix string which format is "group_id user_id" and the
	 * value is last update time.
	 */
	private static HashMap<String, Long> LatestMessageCacheMap = null;

	static {
		LatestMessageCache.LatestMessageCacheMap = new HashMap<String, Long>();
	}

	public static void putMessage(MessageEvent<?> messageEvent) {

		String groupId = messageEvent.getSource().getSenderId();
		
		String userId = messageEvent.getSource().getUserId();
		String prefix = String.format("%s %s", groupId, userId);

		if (!LatestMessageCache.LatestMessageCacheMap.containsKey(prefix)) {
			LatestMessageCache.LatestMessageCacheMap.put(prefix, messageEvent.getTimestamp().toEpochMilli());
			log.info("Cache new message.");
		} else {

			if (messageEvent.getTimestamp().toEpochMilli() > LatestMessageCache.LatestMessageCacheMap.get(prefix)) {
				LatestMessageCache.LatestMessageCacheMap.put(prefix, messageEvent.getTimestamp().toEpochMilli());
				log.info("Replace cached message.");
			}
		}
	}

	public static HashMap<String, Long> getCachedData() {
		return LatestMessageCache.LatestMessageCacheMap;
	}

	public static void clearCacheData() {
		LatestMessageCache.LatestMessageCacheMap.clear();
	}
}
