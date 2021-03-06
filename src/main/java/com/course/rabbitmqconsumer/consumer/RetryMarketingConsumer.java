package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

//@Service
public class RetryMarketingConsumer {

	private static final Logger logger = LoggerFactory.getLogger(RetryMarketingConsumer.class);
	
	private ObjectMapper objectMapper;
	
	public RetryMarketingConsumer() {
		this.objectMapper = new ObjectMapper();
	}
	
	//@RabbitListener(queues = "q.guideline2.marketing.work")
	public void listen(Message message, Channel channel) throws InterruptedException, JsonParseException, IOException{
		Employee employee = objectMapper.readValue(message.getBody(), Employee.class);
		logger.info("Creating employee and publishing on marketing: " + employee);
		channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
	}
	
}
