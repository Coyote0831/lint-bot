package line.bot.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.bot.model.event.CallbackRequest;
import com.linecorp.bot.model.event.JoinEvent;
import com.linecorp.bot.model.event.MessageEvent;

import line.bot.operation.event.JoinEventHandler;
import line.bot.operation.event.MessageEventHandler;

public class CallbackHandler {
	private static Logger log = LoggerFactory.getLogger(CallbackHandler.class);

	public static void eventReceive(CallbackRequest callbackRequest) {

		callbackRequest.getEvents().forEach(event -> {
			if ( event instanceof MessageEvent ) {
				new MessageEventHandler((MessageEvent<?>) event).saveToCache();
			} else if (event instanceof JoinEvent) {
				new JoinEventHandler((JoinEvent) event).createJoinedGroupTable();
			} else {
				log.info(String.format("Unhandle event %s.", event.getClass().getName()));
			}
		});
	}
}
