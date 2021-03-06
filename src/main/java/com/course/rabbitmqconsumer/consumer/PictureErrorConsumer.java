package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Picture;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

//@Service
public class PictureErrorConsumer {

	private ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(PictureErrorConsumer.class);
	
	//@RabbitListener(queues = "q.picture.error")	
	public void listen(Message message, Channel channel) throws JsonParseException, JsonMappingException, IOException {
		Optional<Picture> picture = Optional.empty();
		picture = Optional.of(objectMapper.readValue(message.getBody(), Picture.class));
		
		if (picture.get().getSize() > 9000) {
			//deliverytag, requeue
			channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
		}
		
		picture.ifPresentOrElse(value -> {logger.info("On Picture Image, picture is {}", value);}, () -> logger.info("Picture Image Consumer Error"));		
		channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
	}
	
}
