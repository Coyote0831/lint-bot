package line.bot.operation.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.bot.model.event.JoinEvent;

import line.bot.util.DbHandler;

public class JoinEventHandler {
	
	private static Logger log = LoggerFactory.getLogger(JoinEventHandler.class);
	
	private JoinEvent joinEvent = null;
	
	public JoinEventHandler(JoinEvent joinEvent) {
		this.joinEvent = joinEvent;
	}

	public void createJoinedGroupTable() {
		String groupId = joinEvent.getSource().getSenderId();
		String tableName = String.format("group_%s", groupId);
		try {
			DbHandler.createGroupTable(tableName);
		}catch(RuntimeException e) {
			String errorMsg = String.format("Failed to create table %s, try next time.", tableName);
			log.error(errorMsg, e);
		}
	}
}
