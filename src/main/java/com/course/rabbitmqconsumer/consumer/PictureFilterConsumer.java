package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class PictureFilterConsumer {

	private ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(PictureFilterConsumer.class);
	
	@RabbitListener(queues = "q.picture.filter")	
	public void listen(String message) {
		Optional<Picture> picture = Optional.empty();
		try {
			picture = Optional.of(objectMapper.readValue(message, Picture.class));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		picture.ifPresentOrElse(value -> {logger.info("On Filter, picture is {}", value);}, () -> logger.info("Picture Filter Consumer Error"));			
	}
	
}
