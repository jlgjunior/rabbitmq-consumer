package com.course.rabbitmqconsumer.consumer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.course.rabbitmqconsumer.entity.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class SpringRetryEmployeeConsumer {

	private static final Logger logger = LoggerFactory.getLogger(SpringRetryEmployeeConsumer.class);
	private ObjectMapper objectMapper = new ObjectMapper();
	
	@RabbitListener(queues = "q.spring2.account.work")
	public void listenAccounting(String message) throws JsonMappingException, JsonProcessingException{
		Employee employee = objectMapper.readValue(message, Employee.class);
		
		if (StringUtils.isEmpty(employee.getName())){
			logger.error("On accounting, name is empty: {}", employee);
			throw new IllegalArgumentException("Name is empty");
		}
		
		logger.info("On accounting: {}", employee);	
	}
	
	@RabbitListener(queues = "q.spring2.marketing.work")
	public void listenMarketing(String message) throws JsonMappingException, JsonProcessingException{
		Employee employee = objectMapper.readValue(message, Employee.class);		
		logger.info("On marketing: {}", employee);	
	}
}
