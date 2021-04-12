package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

//@Service
public class RetryImageConsumer {

	private static final String DEAD_EXCHANGE_NAME = "x.guideline.dead";
	
	private static final Logger logger = LoggerFactory.getLogger(RetryImageConsumer.class);
	private DlxProcessingErrorHandler dlxProcessingErrorHandler;
	
	private ObjectMapper objectMapper;
	
	public RetryImageConsumer() {
		this.objectMapper = new ObjectMapper();
		this.dlxProcessingErrorHandler = new DlxProcessingErrorHandler(DEAD_EXCHANGE_NAME);
	}
	
	//@RabbitListener(queues = "q.guideline.image.work")
	public void listen(Message message, Channel channel) throws InterruptedException, JsonParseException, IOException{
		try {
			Picture picture = objectMapper.readValue(message.getBody(), Picture.class);
			if (picture.getSize() > 9000) {
				throw new IOException("Size too large");
			}
			else {
				logger.info("Creating image and publishing: " + picture);
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			}
		}
		catch (IOException e) {
			logger.warn("Error processing image message: " + new String(message.getBody() + " : " + e.getMessage()));
			dlxProcessingErrorHandler.handleErrorProcessingMessage(message, channel);
		}
		
	}
	
}
