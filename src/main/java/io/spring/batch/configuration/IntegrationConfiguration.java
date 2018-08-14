/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.batch.configuration;

import io.spring.batch.domain.Customer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.amqp.core.Queue;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkHandler;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Michael Minella
 */
@Configuration
public class IntegrationConfiguration {

	public static final String CHUNKING_REQUESTS = "chunking.requests";
	public static final String CHUNKING_REPLIES = "chunking.replies";

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic.spring-integration-kafka}")
	private String kafkaTopicForSendingMessage;

	@Value("${kafka.topic.spring-integration-kafka.replies}")
	private String kafkaTopicForSendingReplies;

	@Bean
	public ChunkHandler chunkHandler(TaskletStep step1) throws Exception {
		RemoteChunkHandlerFactoryBean factoryBean = new RemoteChunkHandlerFactoryBean();

		factoryBean.setChunkWriter(chunkWriter());
		factoryBean.setStep(step1);

		return factoryBean.getObject();
	}

	@Bean
	public ChunkMessageChannelItemWriter chunkWriter() {
		ChunkMessageChannelItemWriter chunkWriter = new ChunkMessageChannelItemWriter();

		chunkWriter.setMessagingOperations(messageTemplate());
		chunkWriter.setReplyChannel(inboundReplies());
		chunkWriter.setMaxWaitTimeouts(10);

		return chunkWriter;
	}

	@Bean
	public MessagingTemplate messageTemplate() {
		MessagingTemplate messagingTemplate = new MessagingTemplate(outboundRequests());

		messagingTemplate.setReceiveTimeout(60000000l);

		return messagingTemplate;
	}

	@Bean
	@ServiceActivator(inputChannel = "outboundRequests")
	public MessageHandler kafkaMessageHandler() {

		KafkaProducerMessageHandler<Customer, Customer> handler =
				new KafkaProducerMessageHandler<>(kafkaTemplateSendingMessage());
		handler.setMessageKeyExpression(new LiteralExpression("kafka-integration"));
		handler.setTopicExpression(new LiteralExpression(kafkaTopicForSendingMessage));

		return handler;
	}

	@Bean (name = "kafkaTemplateSendingMessage")
	public KafkaTemplate<Customer, Customer> kafkaTemplateSendingMessage() {
		KafkaTemplate<Customer, Customer> kafkaTemplate = new KafkaTemplate(producerFactory());
		kafkaTemplate.setDefaultTopic(kafkaTopicForSendingMessage);
		return kafkaTemplate;
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JavaSerializer.class);
		// introduce a delay on the send to allow more messages to accumulate
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

		return properties;
	}

	@Bean
	public MessageChannel outboundRequests() {
		return new DirectChannel();
	}

	@Bean
	public Queue requestQueue() {
		return new Queue(CHUNKING_REQUESTS, false);
	}


	@Bean
	@Profile("slave")
	public KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter() {
		KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter<>(kafkaListenerContainer());
		kafkaMessageDrivenChannelAdapter.setOutputChannel(inboundRequests());

		return kafkaMessageDrivenChannelAdapter;
	}

	@SuppressWarnings("unchecked")
	@Bean
	@Profile("slave")
	public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainer() {
		ContainerProperties containerProps = new ContainerProperties(kafkaTopicForSendingMessage);

		return (ConcurrentMessageListenerContainer<String, String>) new ConcurrentMessageListenerContainer<>(
				consumerFactory(), containerProps);
	}

	@Bean
	public ConsumerFactory<?, ?> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JavaDeserializer.class);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-integration");
		// automatically reset the offset to the earliest offset
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return properties;
	}

	@Bean
	public MessageChannel inboundRequests() {
		return new DirectChannel();
	}

	@Bean
	@Profile("slave")
	@ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundReplies")
	public ChunkProcessorChunkHandler chunkProcessorChunkHandler(ItemProcessor<Customer, Customer> upperCaseItemProcessor, ItemWriter<Customer> customerItemWriter) throws Exception {
		SimpleChunkProcessor chunkProcessor = new SimpleChunkProcessor<>(upperCaseItemProcessor, customerItemWriter);
		chunkProcessor.afterPropertiesSet();

		ChunkProcessorChunkHandler<Customer> chunkHandler = new ChunkProcessorChunkHandler<>();

		chunkHandler.setChunkProcessor(chunkProcessor);
		chunkHandler.afterPropertiesSet();

		return chunkHandler;

	}

	@Bean
	public QueueChannel outboundReplies() {
		return new QueueChannel();
	}

	@Bean (name = "kafkaTemplateSendingReplies")
	public KafkaTemplate<Customer, Customer> kafkaTemplateSendingReplies() {
		KafkaTemplate<Customer, Customer> kafkaTemplate = new KafkaTemplate(producerFactory());
		kafkaTemplate.setDefaultTopic(kafkaTopicForSendingReplies);

		return kafkaTemplate;
	}

	@Bean
	@Profile("slave")
	@ServiceActivator(inputChannel = "outboundReplies")
	public MessageHandler kafkaMessageHandlerForOutboundReplies() {

		KafkaProducerMessageHandler<Customer, Customer> handler =
				new KafkaProducerMessageHandler<>(kafkaTemplateSendingReplies());
		handler.setMessageKeyExpression(new LiteralExpression("kafka-integration"));
		handler.setTopicExpression(new LiteralExpression(kafkaTopicForSendingReplies));
		return handler;
	}

	@Bean
	public Queue replyQueue() {
		return new Queue(CHUNKING_REPLIES, false);
	}

	@Bean
	public QueueChannel inboundReplies() {
		return new QueueChannel();
	}

	@Bean
	@Profile("master")
	public KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapterForReplies() {
		KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter<>(kafkaListenerContainerForReplies());
		kafkaMessageDrivenChannelAdapter.setOutputChannel(inboundReplies());

		return kafkaMessageDrivenChannelAdapter;
	}

	@SuppressWarnings("unchecked")
	@Bean
	@Profile("master")
	public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainerForReplies() {
		ContainerProperties containerProps = new ContainerProperties(kafkaTopicForSendingReplies);

		return (ConcurrentMessageListenerContainer<String, String>) new ConcurrentMessageListenerContainer<>(
				consumerFactory(), containerProps);
	}



	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	public PollerMetadata defaultPoller() {
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(10));
		return pollerMetadata;
	}
}
