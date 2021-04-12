package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

@Service
public class RetryAccountingConsumer {

	private static final String DEAD_EXCHANGE_NAME = "x.guideline2.dead";
	private static final String ROUTING_KEY = "accounting";
	
	private static final Logger logger = LoggerFactory.getLogger(RetryAccountingConsumer.class);
	private DlxFanoutProcessingErrorHandler dlxProcessingErrorHandler;
	
	private ObjectMapper objectMapper;
	
	public RetryAccountingConsumer() {
		this.objectMapper = new ObjectMapper();
		this.dlxProcessingErrorHandler = new DlxFanoutProcessingErrorHandler(DEAD_EXCHANGE_NAME, ROUTING_KEY);
	}
	
	@RabbitListener(queues = "q.guideline2.accounting.work")
	public void listen(Message message, Channel channel) throws InterruptedException, JsonParseException, IOException{
		try {
			Employee employee = objectMapper.readValue(message.getBody(), Employee.class);
			if (StringUtils.isEmpty(employee.getName())) {
				throw new IllegalArgumentException("Invalid name");
			}
			else {
				logger.info("Creating employee and publishing on accounting: " + employee);
				channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			}
		}
		catch (Exception e) {
			logger.warn("Error processing accounting employee message: " + new String(message.getBody() + " : " + e.getMessage()));
			dlxProcessingErrorHandler.handleErrorProcessingMessage(message, channel);
		}
		
	}
	
}
