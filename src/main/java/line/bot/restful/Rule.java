package line.bot.restful;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import line.bot.operation.RuleHandler;

@Produces
@Path("/rule")
public class Rule {
	private static Logger log = LoggerFactory.getLogger(Rule.class);

	@GET
	public Response getRules() {

		JsonObject result = new JsonObject();
		result.add("start_with_rule", RuleHandler.listStartWithRule());

		return Response.ok().header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON).entity(result.toString())
				.build();
	}

	@POST
	public Response createRules(InputStream body) {

		JsonObject input = null;
		try {
			input = new JsonParser().parse(IOUtils.toString(body, StandardCharsets.UTF_8.name())).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (input == null) {
			return Response.serverError().build();
		}
		
		log.info(String.format("Received create data : %s", input.toString()));
		
		if ( input.has("deltag") ) {
			return Response.ok().build();
		}

		String startWith = input.get("start_with").getAsString().trim();
		String replyText = input.get("reply_text").getAsString();
		long textLength = startWith.length()+3;
		
		log.info(String.format("start_with : [%s]", startWith));
		log.info(String.format("reply_text : [%s]", replyText));
		log.info(String.format("text_length : [%s]", textLength));

		RuleHandler.createStartWithRule(startWith, replyText, textLength);

		return Response.ok().build();
	}

	@DELETE
	public Response deleteRules(InputStream body) {

		JsonObject input = null;
		try {
			input = new JsonParser().parse(IOUtils.toString(body, StandardCharsets.UTF_8.name())).getAsJsonObject();
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (input == null) {
			return Response.serverError().build();
		}
		
		log.info(String.format("Received delete data : %s", input.toString()));

		String startWith = input.get("start_with").getAsString();
		String replyText = input.get("reply_text").getAsString();
		long textLength = input.get("text_length").getAsLong();
		
		log.info(String.format("start_with : [%s]", startWith));
		log.info(String.format("reply_text : [%s]", replyText));
		log.info(String.format("text_length : [%s]", textLength));

		RuleHandler.deleteStartWithRule(startWith, replyText, textLength);

		return Response.ok().build();
	}
}
