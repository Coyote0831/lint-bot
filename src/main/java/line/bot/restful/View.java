package line.bot.restful;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import line.bot.operation.ViewHandler;

@Produces
@Path("/view")
public class View {

	private static Logger log = LoggerFactory.getLogger(View.class);

	private static final String DEFAULT_FORMAT = "json";

	@GET
	public Response getView(@QueryParam("format") String format) {

		if (StringUtils.isEmpty(format)) {
			format = View.DEFAULT_FORMAT;
		}

		Object result = ViewHandler.getViewData(format);

		return Response.ok().header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
				.entity(((JsonObject) result).toString()).build();
	}
}
