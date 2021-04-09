package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class EmployeeJsonConsumer {

	private ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(EmployeeJsonConsumer.class);
	
	@RabbitListener(queues = "course.employee")
	public void listen(String message) {
		Optional<Employee> employee = null;
		try {
			employee = Optional.of(objectMapper.readValue(message, Employee.class));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		employee.ifPresentOrElse(value -> {logger.info(value.toString());}, () -> logger.info("Error"));		
	}
	
}
