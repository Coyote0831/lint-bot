package line.bot.restful;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.bot.client.LineSignatureValidator;
import com.linecorp.bot.model.event.CallbackRequest;

import line.bot.operation.CallbackHandler;
import line.bot.property.LineProperty;

@Produces
@Path("/callback")
public class CallBack {

	private static Logger log = LoggerFactory.getLogger(CallBack.class);

	@Context
	private HttpHeaders httpHeaders;

	private final static String X_LINE_SIGNATURE_HEADER = "X-Line-Signature";

	private static LineSignatureValidator lineSignatureValidator = null;
	private static ObjectMapper objectMapper = null;

	static {
		lineSignatureValidator = new LineSignatureValidator(LineProperty.getChannelAccessToken().getBytes());
		objectMapper = buildObjectMapper();
	}

	@POST
	@Produces("application/json")
	public Response callBackApi(InputStream body) {

		String signature = this.httpHeaders.getHeaderString(CallBack.X_LINE_SIGNATURE_HEADER);

		CallbackRequest callbackRequest = null;
		
		String bodyString = StringUtils.EMPTY;
		try {
			bodyString = IOUtils.toString(body, StandardCharsets.UTF_8.name());
		} catch (IOException e1) {
		}
		log.info(String.format("Received event : %s", bodyString));

		try {
			callbackRequest = handle(signature, bodyString);
		} catch (LineBotCallbackException | IOException e) {
			String errorMsg = "Failed to get call back request.";
			log.error(errorMsg, e);
			Response.serverError().build();
		}

		CallbackHandler.eventReceive(callbackRequest);

		return Response.ok().build();
	}

	/**
	 * Parse request.
	 *
	 * @param signature
	 *            X-Line-Signature header.
	 * @param payload
	 *            Request body.
	 * @return Parsed result. If there's an error, this method sends response.
	 * @throws LineBotCallbackException
	 *             There's an error around signature.
	 */
	public CallbackRequest handle(String signature, String payload) throws LineBotCallbackException, IOException {
		// validate signature
		if (signature == null || signature.length() == 0) {
			String errorMsg = "Missing 'X-Line-Signature' header";
			log.error(errorMsg);
			throw new LineBotCallbackException(errorMsg);
		}

		final byte[] json = payload.getBytes(StandardCharsets.UTF_8);

		/*
		if (!lineSignatureValidator.validateSignature(json, signature)) {
			String errorMsg = "Invalid API signature";
			log.error(errorMsg);
			throw new LineBotCallbackException(errorMsg);
		}*/

		final CallbackRequest callbackRequest = objectMapper.readValue(json, CallbackRequest.class);
		if (callbackRequest == null || callbackRequest.getEvents() == null) {
			String errorMsg = "Invalid content";
			log.error(errorMsg);
			throw new LineBotCallbackException(errorMsg);
		}
		return callbackRequest;
	}

	private static ObjectMapper buildObjectMapper() {
		final ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		// Register JSR-310(java.time.temporal.*) module and read number as millsec.
		objectMapper.registerModule(new JavaTimeModule())
				.configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
		return objectMapper;
	}

	public class LineBotCallbackException extends Exception {
		private static final long serialVersionUID = -950894346433317253L;

		public LineBotCallbackException(String message) {
			super(message);
		}
	}
}
