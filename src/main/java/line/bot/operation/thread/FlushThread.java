package line.bot.operation.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import line.bot.util.DbHandler;
import line.bot.util.LatestMessageCache;

public class FlushThread implements Runnable {

	private static Logger log = LoggerFactory.getLogger(FlushThread.class);

	@Override
	public void run() {
		log.info("Start to flush data.");
		this.flush();
	}

	private void flush() {

		LatestMessageCache.getCachedData().entrySet().forEach(entrySet -> {

			String prefix = entrySet.getKey();

			String groupId = prefix.split(" ")[0];
			String userId = prefix.split(" ")[1];

			long lastMessageTime = entrySet.getValue();

			DbHandler.updateLatestMessage(String.format("group_%s", groupId), userId, lastMessageTime);
		});

		try {
			DbHandler.flush();
		} catch (RuntimeException e) {
			String errorMsg = "Failed to flush data. Maybe next time.";
			log.error(errorMsg, e);
			return;
		}

		LatestMessageCache.clearCacheData();
		log.info("All data flushed.");
	}
}
