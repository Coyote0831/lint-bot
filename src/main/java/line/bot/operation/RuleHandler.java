package line.bot.operation;

import java.util.LinkedList;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.linecorp.bot.client.LineMessagingClientBuilder;
import com.linecorp.bot.model.ReplyMessage;
import com.linecorp.bot.model.event.MessageEvent;
import com.linecorp.bot.model.event.message.TextMessageContent;
import com.linecorp.bot.model.message.Message;
import com.linecorp.bot.model.message.TextMessage;

import line.bot.property.LineProperty;
import line.bot.util.DbHandler;

public class RuleHandler {

	private static Logger log = LoggerFactory.getLogger(RuleHandler.class);

	private static JsonArray ruleStartWithJsonArray = null;

	static {
		RuleHandler.ruleStartWithJsonArray = new JsonArray();
		RuleHandler.ruleStartWithJsonArray = DbHandler.getStartWithRuleTableDatas();
	}

	public static void executeMessageRule(MessageEvent<?> messageEvent) {
		log.info("Start to execute 'StartString' rule.");
		RuleHandler.executeStartWith(messageEvent);
		log.info("Finish 'StartString' rule.");
	}

	public static void createStartWithRule(String startWith, String replyText, long textLength) {
		startWith = startWith.trim();
		replyText = replyText.trim();

		JsonObject newRule = new JsonObject();

		newRule.addProperty("start_with", startWith);
		newRule.addProperty("reply_text", replyText);
		newRule.addProperty("text_length", textLength);

		RuleHandler.ruleStartWithJsonArray.add(newRule);

		DbHandler.insertStartWithRule(startWith, replyText, textLength);
	}

	public static void deleteStartWithRule(String startWith, String replyText, long textLength) {

		JsonObject delete = new JsonObject();

		delete.addProperty("start_with", startWith);
		delete.addProperty("reply_text", replyText);
		delete.addProperty("text_length", textLength);

		RuleHandler.ruleStartWithJsonArray.remove(delete);

		DbHandler.deleteStartWithRule(startWith, replyText, textLength);
	}

	public static JsonArray listStartWithRule() {
		return RuleHandler.ruleStartWithJsonArray;
	}

	private static void executeStartWith(MessageEvent<?> messageEvent) {

		LinkedList<Message> messageList = new LinkedList<Message>();
		RuleHandler.ruleStartWithJsonArray.forEach(action -> {

			JsonObject rule = action.getAsJsonObject();

			if ((messageEvent.getMessage() instanceof TextMessageContent)) {

				String receivedText = ((TextMessageContent) messageEvent.getMessage()).getText();
				String ruleText = rule.get("start_with").getAsString();
				long ruleLength = rule.get("text_length").getAsLong();

				if ((receivedText.startsWith("＠") || receivedText.startsWith("@"))
						&& (ruleText.startsWith("＠") || ruleText.startsWith("@"))) {
					receivedText = receivedText.substring(1);
					ruleText = ruleText.substring(1);
					ruleLength = ruleLength - 1;
				}

				if ((receivedText.startsWith(ruleText)) && (receivedText.length() <= ruleLength)) {
					TextMessage message = new TextMessage(rule.get("reply_text").getAsString());
					messageList.add(message);
				}
			}
		});

		if (messageList.size() > 0) {
			LineMessagingClientBuilder lineMessagingClientBuilder = new LineMessagingClientBuilder(
					LineProperty.getChannelAccessToken());
			ReplyMessage replyMessage = new ReplyMessage(messageEvent.getReplyToken(), messageList);
			try {
				lineMessagingClientBuilder.build().replyMessage(replyMessage).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			lineMessagingClientBuilder.removeAllInterceptors();
			lineMessagingClientBuilder = null;
		}
	}
}
