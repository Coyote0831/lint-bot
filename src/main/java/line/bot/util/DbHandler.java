package line.bot.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import line.bot.property.LineProperty;

public class DbHandler {

	private static Logger log = LoggerFactory.getLogger(DbHandler.class);

	private static LinkedList<String> commandInsertUserList = null;
	private static LinkedList<String> commandDeletetUserList = null;
	
	private static Connection sqlConnection = null;
	private static Statement sqlStatement = null;
	
	private static ReadWriteLock lock = null;
	
	private static final String insertUserIntoGroupCommand = 
			"INSERT INTO {table_name} (USER_ID, LAST_UPDATE_TIME) "+
			"VALUES ('{user_id}', {last_update_time})";
	
	private static final String deleteUserFromGroupCommand = 
			"DELETE FROM {table_name} WHERE USER_ID = '{user_id}'";

	private static final String createGroupTableCommand = 
			"CREATE TABLE IF NOT EXISTS {table_name} "+
		    "(USER_ID               TEXT       NOT NULL, "+
		    "LAST_UPDATE_TIME       bigint     NOT NULL)";
	
	private static final String createStartMatchRuleTableCommand =
			"CREATE TABLE IF NOT EXISTS start_match_rule "+
			"(START_WITH    TEXT      NOT NULL, "+
			"REPLY_TEXT     TEXT      NOT NULL, "+
			"TEXT_LENGTH    bigint    NOT NULL)";
	
	private static final String insertStartMatchRuleCommand = 
			"INSERT INTO start_match_rule (START_WITH, REPLY_TEXT, TEXT_LENGTH) "+
			"VALUES ('{start_with}', '{reply_text}', {text_length})";
	
	private static final String deleteStartMatchRuleCommand = 
			"DELETE FROM start_match_rule WHERE START_WITH = '{start_with}' AND REPLY_TEXT = '{reply_text}' AND TEXT_LENGTH = {text_length}";
	
	private static final String listTableCommand = 
			"SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'";
	
	private static final String selectAllUserDataCommand =
			"SELECT USER_ID, LAST_UPDATE_TIME FROM {table_name}";
	
	private static final String selectAllStartWithRuleCommand =
			"SELECT START_WITH, REPLY_TEXT, TEXT_LENGTH FROM {table_name}";
	
	private DbHandler() {
	}

	public static synchronized void initiate() throws ClassNotFoundException {
		DbHandler.commandInsertUserList = new LinkedList<String>();
		DbHandler.commandDeletetUserList = new LinkedList<String>();
		DbHandler.lock = new ReentrantReadWriteLock();
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			String errorMsg = "Failed to init db handler due to driver load failed.";
			log.error(errorMsg, e);
		}
	}

	public static void updateLatestMessage(String tableName, String userId, long timestamp) {
		tableName = tableName.toLowerCase();

		DbHandler.commandInsertUserList.add(DbHandler.insertUserIntoGroupCommand.replace("{table_name}", tableName)
				.replace("{user_id}", userId).replace("{last_update_time}", String.valueOf(timestamp)));
		DbHandler.commandDeletetUserList.add(DbHandler.deleteUserFromGroupCommand.replace("{table_name}", tableName)
				.replace("{user_id}", userId));
	}
	
	public static void flush() throws RuntimeException {
		
		String action = "flush data";
		
		if (DbHandler.commandInsertUserList.size() == 0 || DbHandler.commandDeletetUserList.size() == 0) {
			log.info("No data need to be flushed.");
			return;
		}
		
		try {
			batchDeleteUser();
			batchInsertUser();
		}catch(Exception e) {
			String errorMsg = String.format("Failed to %s due to batch error. Give up and clear data.", action);
			log.error(errorMsg, e);
			DbHandler.commandInsertUserList.clear();
			DbHandler.commandDeletetUserList.clear();
		}
	}
	
	private static void batchDeleteUser() {
		String action = "batch delete user";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		try {
			log.info(String.format("Start to %s.", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();

			DbHandler.commandDeletetUserList.forEach(command -> {
				try {
					DbHandler.sqlStatement.addBatch(command);
				} catch (SQLException e) {
					String errorMsg = String.format("Failed to add batch with command %s", command);
					log.error(errorMsg, e);
				}
			});
			
			try {
				DbHandler.sqlStatement.executeBatch();
			} catch (SQLException e) {
				String errorMsg = String.format("Failed to %s due to db failed.", action);
				log.error(errorMsg, e);
				throw new RuntimeException(e);
			}
			
			log.info("Bath user deleted.");
			DbHandler.sqlStatement.clearBatch();
			DbHandler.commandDeletetUserList.clear();
			
		} catch (SQLException | RuntimeException e) {
			String errorMsg = String.format("Failed to %s due to db access error. Clear query and return.", action);
			log.error(errorMsg, e);
			
			try {
				DbHandler.sqlStatement.clearBatch();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			DbHandler.commandDeletetUserList.clear();
			return;
		} 
		
		finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	private static void batchInsertUser() {
		String action = "batch insert user";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		try {
			log.info(String.format("Start to %s.", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();

			DbHandler.commandInsertUserList.forEach(command -> {
				try {
					DbHandler.sqlStatement.addBatch(command);
				} catch (SQLException e) {
					String errorMsg = String.format("Failed to add batch with command %s", command);
					log.error(errorMsg, e);
				}
			});
			
			try {
				DbHandler.sqlStatement.executeBatch();
			} catch (SQLException e) {
				String errorMsg = String.format("Failed to %s due to db failed.", action);
				log.error(errorMsg, e);
				throw new RuntimeException(e);
			}
			
			log.info("Bath user created.");
			DbHandler.sqlStatement.clearBatch();
			DbHandler.commandInsertUserList.clear();
			
		} catch (SQLException | RuntimeException e) {
			String errorMsg = String.format("Failed to %s due to db access error. Clear query and return.", action);
			log.error(errorMsg, e);
			
			try {
				DbHandler.sqlStatement.clearBatch();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			DbHandler.commandInsertUserList.clear();
			return;
		} 
		
		finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	public static void createGroupTable(String tableName) throws RuntimeException {
		
		tableName = tableName.toLowerCase();
		
		String action = "create group table";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		try {
			log.info(String.format("Start to %s %s", action, tableName));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			DbHandler.sqlStatement.execute(DbHandler.createGroupTableCommand.replace("{table_name}", tableName));
			log.info(String.format("Table %s created.", tableName));
		} catch (SQLException e) {
			String errorMsg = String.format("Failed to %s due to db access error with command : %s", action,
					DbHandler.createGroupTableCommand.replace("{table_name}", tableName));
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		} finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	public static void createStartMatchRuleTable() throws RuntimeException {
		
		String action = "create start match rule table";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		try {
			log.info(String.format("Start to %s.", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			DbHandler.sqlStatement.execute(DbHandler.createStartMatchRuleTableCommand);
			log.info("Table created.");
		} catch (SQLException e) {
			String errorMsg = String.format("Failed to %s due to db access error with command : %s", action,
					DbHandler.createStartMatchRuleTableCommand);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		} finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	public static void insertStartWithRule(String startWith, String replyText, long textLength)
			throws RuntimeException {
		
		String action = "insert rule";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		try {
			log.info(String.format("Start to %s.", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			
			DbHandler.sqlStatement.execute(DbHandler.insertStartMatchRuleCommand
					.replace("{start_with}", startWith.trim()).replace("{reply_text}", replyText.trim())
					.replace("{text_length}", String.valueOf(textLength)));
			
			log.info("Rule created.");
			
		} catch (SQLException | RuntimeException e) {
			String errorMsg = String.format("Failed to %s due to db access error.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(e);
		} 
		
		finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	public static void deleteStartWithRule(String startWith, String replyText, long textLength)
			throws RuntimeException {
		
		String action = "delete rule";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		try {
			log.info(String.format("Start to %s.", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			
			DbHandler.sqlStatement.execute(DbHandler.deleteStartMatchRuleCommand
					.replace("{start_with}", startWith.trim()).replace("{reply_text}", replyText.trim())
					.replace("{text_length}", String.valueOf(textLength)));
			
			log.info("Rule deleted.");
			
		} catch (SQLException | RuntimeException e) {
			String errorMsg = String.format("Failed to %s due to db access error.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(e);
		} 
		
		finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
	}
	
	public static List<String> listTables(){
		String action = "list tables";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		ResultSet queryResult = null;
		List<String> resultList = new LinkedList<String>();
		
		try {
			log.info(String.format("Start to %s", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			
			queryResult = DbHandler.sqlStatement.executeQuery(DbHandler.listTableCommand);
			
			while(queryResult.next()) {
				resultList.add(queryResult.getString(1));
			}
			
			log.info(String.format("%s tables reached.", resultList.size()));
		} catch (SQLException e) {
			String errorMsg = String.format("Failed to %s due to db access error with command : %s", action,
					DbHandler.listTableCommand);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		} finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
				queryResult.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
		return resultList;
	}
	
	public static JsonArray getGroupTableDatas(String tableName) {
		
		tableName = tableName.toLowerCase();
		
		String action = "get group table datas";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		ResultSet queryResult = null;
		
		TreeSet<JsonObject> sort = new TreeSet<JsonObject>(defaultTimestampComparator);
		
		try {
			log.info(String.format("Start to %s", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			
			queryResult = DbHandler.sqlStatement.executeQuery(DbHandler.selectAllUserDataCommand.replace("{table_name}", tableName));
			
			while (queryResult.next()) {
				JsonObject json = new JsonObject();
				json.addProperty("user_id", queryResult.getString("USER_ID").trim());
				json.addProperty("timestamp",queryResult.getLong("LAST_UPDATE_TIME"));
				sort.add(json);
			}
			
			log.info(String.format("%s datas of table %s reached.", sort.size(), tableName));
		} catch (SQLException e) {
			String errorMsg = String.format("Failed to %s due to db access error with command : %s", action,
					DbHandler.listTableCommand);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		} finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
				queryResult.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
		
		JsonArray resultJsonArray = new JsonArray();
		
		sort.forEach(json->{
			JsonObject sorted = new JsonObject();
			
			sorted.addProperty("user_id", json.get("user_id").getAsString());
			sorted.addProperty("timestamp",LocalDateTime.ofInstant(Instant.ofEpochMilli(json.get("timestamp").getAsLong()),
					ZoneId.of("Asia/Taipei")).toString());
			resultJsonArray.add(sorted);
		});
		
		sort.clear();
		sort = null;
		
		return resultJsonArray;
	}
	
	public static JsonArray getStartWithRuleTableDatas() {
		
		String tableName = "start_match_rule";
		
		String action = "get start with rule table datas";
		
		try {
			lock.writeLock().tryLock(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			String errorMsg = String.format("Failed to %s due to lock failed.", action);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		}
		
		ResultSet queryResult = null;
		JsonArray resultJsonArray = new JsonArray();
		
		try {
			log.info(String.format("Start to %s", action));
			DbHandler.sqlConnection = DriverManager.getConnection(LineProperty.getDbConnectionUri(),
					LineProperty.getDbUserName(), LineProperty.getDbPassword());
			DbHandler.sqlStatement = DbHandler.sqlConnection.createStatement();
			
			queryResult = DbHandler.sqlStatement.executeQuery(DbHandler.selectAllStartWithRuleCommand.replace("{table_name}", tableName));
			
			while (queryResult.next()) {
				JsonObject json = new JsonObject();
				json.addProperty("start_with", queryResult.getString("START_WITH"));
				json.addProperty("reply_text", queryResult.getString("REPLY_TEXT"));
				json.addProperty("text_length", queryResult.getLong("TEXT_LENGTH"));
				resultJsonArray.add(json);
			}
			
			log.info(String.format("%s datas of table %s reached.", resultJsonArray.size(), tableName));
		} catch (SQLException e) {
			String errorMsg = String.format("Failed to %s due to db access error with command : %s", action,
					DbHandler.listTableCommand);
			log.error(errorMsg, e);
			throw new RuntimeException(errorMsg);
		} finally {
			lock.writeLock().unlock();
			try {
				DbHandler.sqlStatement.close();
				DbHandler.sqlConnection.close();
				queryResult.close();
			} catch (SQLException e) {
				String errorMsg = "Failed to close sql.";
				log.error(errorMsg, e);
			}
		}
		return resultJsonArray;
	}
	
	private static final Comparator<JsonObject> defaultTimestampComparator =
		      new Comparator<JsonObject>() {
		        @Override
		        public int compare(JsonObject obj1, JsonObject obj2) {
		          String str1 = String.valueOf(obj1.get("timestamp").getAsLong());
		          String str2 = String.valueOf(obj2.get("timestamp").getAsLong());
		          if (StringUtils.isBlank(str2)) {
		            return 1;
		          }
		          if (StringUtils.isBlank(str1)) {
		            return -1;
		          }
		          return str1.compareTo(str2);
		        }
		      };
}
