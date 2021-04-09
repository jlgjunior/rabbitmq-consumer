package com.course.rabbitmqconsumer.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class FixedRateConsumer {

	private final Logger logger = LoggerFactory.getLogger(FixedRateConsumer.class);
	
	@RabbitListener(queues = "course.fixedrate")
	public void listen(String message) {
		logger.info("Consuming {}", message);
	}
}
