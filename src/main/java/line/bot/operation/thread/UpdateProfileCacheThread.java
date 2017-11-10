package line.bot.operation.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import line.bot.operation.ViewHandler;
import line.bot.util.UserProfileCache;

public class UpdateProfileCacheThread implements Runnable{
	
	private static Logger log = LoggerFactory.getLogger(UpdateProfileCacheThread.class);

	@Override
	public void run() {
		
		UserProfileCache.clearCache();
		
		ViewHandler.getJsonViewData();
		log.info("Cache updated.");
	}

}
