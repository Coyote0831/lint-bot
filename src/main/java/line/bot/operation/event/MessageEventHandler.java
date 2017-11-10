package line.bot.operation.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.bot.model.event.MessageEvent;

import line.bot.operation.RuleHandler;
import line.bot.util.LatestMessageCache;

public class MessageEventHandler {

	private static Logger log = LoggerFactory.getLogger(MessageEventHandler.class);

	private MessageEvent<?> messageEvent = null;
	
	public MessageEventHandler(MessageEvent<?> messageEvent) {
		this.messageEvent = messageEvent;
	}

	public void saveToCache() {
		LatestMessageCache.putMessage(messageEvent);
		RuleHandler.executeMessageRule(messageEvent);
	}
}
