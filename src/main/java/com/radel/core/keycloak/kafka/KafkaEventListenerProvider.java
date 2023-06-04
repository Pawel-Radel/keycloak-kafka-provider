package com.radel.core.keycloak.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.keycloak.models.KeycloakSession;

public class KafkaEventListenerProvider implements EventListenerProvider {

	private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

	private final KeycloakSession keycloakSession;
	private String topicEvents;

	private List<EventType> events;

	private String topicAdminEvents;

	private Producer<String, String> producer;

	private ObjectMapper mapper;

	public KafkaEventListenerProvider(String bootstrapServers, String clientId, String topicEvents, String[] events,
									  String topicAdminEvents, Map<String, Object> kafkaProducerProperties, KafkaProducerFactory factory, KeycloakSession session) {
		LOG.info("Topic events" + topicEvents);
		LOG.info("Topic admin events" + topicAdminEvents);
		LOG.info("events" + events);

		this.topicEvents = topicEvents;
		this.events = new ArrayList<>();
		this.topicAdminEvents = topicAdminEvents;
		this.keycloakSession = session;

		for (String event : events) {
			try {
				EventType eventType = EventType.valueOf(event.toUpperCase());
				LOG.info(String.format("Adding event: %s", event));
				this.events.add(eventType);
			} catch (IllegalArgumentException e) {
				LOG.info("Ignoring event >" + event + "<. Event does not exist.");
			}
		}

		producer = factory.createProducer(clientId, bootstrapServers, kafkaProducerProperties);
		mapper = new ObjectMapper();
	}

	private void produceEvent(String eventAsString, String topic)
			throws InterruptedException, ExecutionException, TimeoutException {
		LOG.info("Produce to topic: " + topicEvents + " ...");
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);
		Future<RecordMetadata> metaData = producer.send(record);
		RecordMetadata recordMetadata = metaData.get(30, TimeUnit.SECONDS);
		LOG.info("Produced to topic: " + recordMetadata.topic());
	}

	@Override
	public void onEvent(Event event) {

		String name = this.keycloakSession.realms().getRealm(event.getRealmId()).getName();

		if (events.contains(event.getType())) {
			try {
				LOG.info("On event method (try):" + name);
				produceEvent(mapper.writeValueAsString(event), topicEvents);
			} catch (JsonProcessingException | ExecutionException | TimeoutException e) {
				LOG.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void onEvent(AdminEvent event, boolean includeRepresentation) {

		String name = this.keycloakSession.realms().getRealm(event.getRealmId()).getName();

		if(topicAdminEvents == null) {
			LOG.warn("Admin events are null");
		}

		if (topicAdminEvents != null) {
			try {
				produceEvent(mapper.writeValueAsString(event), "admin." + name);
			} catch (JsonProcessingException | ExecutionException | TimeoutException e) {
				LOG.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void close() {
		// ignore
	}
}
