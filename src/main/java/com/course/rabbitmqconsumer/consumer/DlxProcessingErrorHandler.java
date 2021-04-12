package com.course.rabbitmqconsumer.consumer;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.lang.NonNull;

import com.course.rabbitmqconsumer.rabbitmq.RabbitmqHeader;
import com.rabbitmq.client.Channel;

public class DlxProcessingErrorHandler {

	private static final Logger logger = LoggerFactory.getLogger(DlxProcessingErrorHandler.class);

	@NonNull
	private String deadExchangeName;

	private int maxRetryCount = 3;

	public DlxProcessingErrorHandler(String deadExchangeName) throws IllegalArgumentException {
		super();

		if (StringUtils.isAnyEmpty(deadExchangeName)) {
			throw new IllegalArgumentException("Must define dlx exchange name");
		}

		this.deadExchangeName = deadExchangeName;
	}

		
	public DlxProcessingErrorHandler(String deadExchangeName, int maxRetryCount) {
		this(deadExchangeName);
		setMaxRetryCount(maxRetryCount);
	}

	public boolean handleErrorProcessingMessage(Message message, Channel channel) {
		RabbitmqHeader rabbitmqHeader = new RabbitmqHeader(message.getMessageProperties().getHeaders());
		
		try {
			if (rabbitmqHeader.getFailedRetryCount() <= maxRetryCount) {
				logger.warn("[DEAD] Error at " + new Date() + " on retry " + rabbitmqHeader.getFailedRetryCount() + " for message " + message);
					channel.basicPublish(getDeadExchangeName(), message.getMessageProperties().getReceivedRoutingKey(), null, message.getBody());
					channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			}
			else {
				logger.debug("[REQUEUE] Error at " + new Date() + " on retry " + rabbitmqHeader.getFailedRetryCount() + " for message " + message);
				channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
			}
			return true;
		}
		catch (IOException e) {
			logger.warn("[HANDLER-FAILED] Error at " + new Date() + " on retry " + rabbitmqHeader.getFailedRetryCount() + " for message " + message);
		}
		return false;
	}
	
	public String getDeadExchangeName() {
		return deadExchangeName;
	}

	public int getMaxRetryCount() {
		return maxRetryCount;
	}

	public void setDeadExchangeName(String deadExchangeName) {
		this.deadExchangeName = deadExchangeName;
	}

	public void setMaxRetryCount(int maxRetryCount) throws IllegalArgumentException{
		if (maxRetryCount > 1000) {
			throw new IllegalArgumentException("max retry must be between [0,1000]");
		}
		
		this.maxRetryCount = maxRetryCount;
	}
}
