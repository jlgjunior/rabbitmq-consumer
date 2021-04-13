package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Service
public class SpringRetryPictureProducer {

	@Autowired
	RabbitTemplate rabbitTemplate;
	private ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(SpringRetryPictureProducer.class);
	
	//@RabbitListener(queues = "q.spring.image.work")
	public void listenImage(String message) throws IOException {
		Picture picture = objectMapper.readValue(message, Picture.class);
		logger.info("Consuming image {}", picture.getName());
		
		if (picture.getSize() > 9000) {
			throw new IOException("Image " + picture.getName() + " size too large : " + picture.getSize());
		}
		
		logger.info("Creating thumbnail for image {}", picture.getName());
	}
	
	//@RabbitListener(queues = "q.spring.vector.work")
	public void listenVector(String message) throws JsonMappingException, JsonProcessingException {

		Picture picture = objectMapper.readValue(message, Picture.class);
		logger.info("Consuming vector {}", picture.getName());	
		logger.info("Creating vector for image {}", picture.getName());
	}
}
