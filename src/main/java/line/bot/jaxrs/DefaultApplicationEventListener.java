package line.bot.jaxrs;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import line.bot.operation.thread.FlushThread;
import line.bot.operation.thread.UpdateProfileCacheThread;
import line.bot.property.LineProperty;
import line.bot.util.DbHandler;

public class DefaultApplicationEventListener implements ApplicationEventListener {
	private Logger log = LoggerFactory.getLogger(DefaultApplicationEventListener.class);

	private static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = null;

	@Override
	public void onEvent(ApplicationEvent event) {
		switch (event.getType()) {
		case INITIALIZATION_APP_FINISHED:
			log.debug("Initiate the Line property...");
			try {
				LineProperty.initiate();
			} catch (IOException e) {
				String errorMsg = "Initiate the Line property was failed";
				log.error(errorMsg, e);
				throw new RuntimeException(errorMsg);
			}

			log.debug("Initiate the db handler...");
			try {
				DbHandler.initiate();
			} catch (ClassNotFoundException e) {
				String errorMsg = "Initiate the Ldb handler was failed";
				log.error(errorMsg, e);
				throw new RuntimeException(errorMsg);
			}

			DbHandler.createStartMatchRuleTable();

			DefaultApplicationEventListener.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2);

			DefaultApplicationEventListener.scheduledThreadPoolExecutor.scheduleAtFixedRate(new FlushThread(), 0, 1,
					TimeUnit.MINUTES);

			DefaultApplicationEventListener.scheduledThreadPoolExecutor
					.scheduleAtFixedRate(new UpdateProfileCacheThread(), 0, 1, TimeUnit.HOURS);
			break;

		default:
			// No action should be applied here.
		}
	}

	@Override
	public RequestEventListener onRequest(RequestEvent requestEvent) {
		return null;
	}
}