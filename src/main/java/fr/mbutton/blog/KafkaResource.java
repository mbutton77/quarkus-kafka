package fr.mbutton.blog;

import io.smallrye.reactive.messaging.kafka.KafkaMessage;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.util.concurrent.CompletionStage;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/consume")
public class KafkaResource {

	private OutboundSseEvent.Builder eventBuilder;
	private Sse sse;
	private SseEventSink sseEventSink = null;
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Context
	public void setSse (Sse sse) {
		this.sse = sse;
		this.eventBuilder = sse.newEventBuilder();
	}

	@GET
	@Produces(MediaType.SERVER_SENT_EVENTS)
	public void consume (@Context SseEventSink sseEventSink) {
		this.sseEventSink = sseEventSink;
	}

	@Incoming("events")
	public CompletionStage<Void> onMessage (KafkaMessage<String, String> message) throws IOException {
		logger.debug("Message content {}", message.getPayload());
		if (sseEventSink != null) {
			String display = String.valueOf(LocalTime.now().toString()) + " | "  + getHost() + " | " + message.getPayload();
			OutboundSseEvent sseEvent = this.eventBuilder
					.name("message")
					.id(message.getKey())
					.mediaType(MediaType.TEXT_PLAIN_TYPE)
					.data(display)
					.reconnectDelay(3000)
					.comment(message.getPayload())
					.build();
			sseEventSink.send(sseEvent);
		}
		return message.ack();
	}

	private String getHost () {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			logger.error("Problem while trying to get the hostname", ex);
			return "Unknown";
		}
	}
}