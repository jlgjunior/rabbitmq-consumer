package com.course.rabbitmqconsumer.consumer;

import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

//@Service
public class FixedRateConsumer {

	private final Logger logger = LoggerFactory.getLogger(FixedRateConsumer.class);
	
	//@RabbitListener(queues = "course.fixedrate", concurrency = "3")
	public void listen(String message) {
		logger.info("Consuming {} on thread {}", message, Thread.currentThread().getName());
		try {
			Thread.sleep(ThreadLocalRandom.current().nextLong(2000));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
