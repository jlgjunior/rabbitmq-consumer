package com.course.rabbitmqconsumer.rabbitmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

public class RabbitmqHeader {

	private static final String KEYWORD_QUEUE_WAIT = "wait";
	private List<RabbitmqHeaderXDeath> xDeaths = new ArrayList<>(2);
	private String xFirstDeathExchange = StringUtils.EMPTY;
	private String xFirstDeathQueue = StringUtils.EMPTY;
	private String xFirstDeathReason = StringUtils.EMPTY;

	public RabbitmqHeader(Map<String, Object> headers) {
		if (headers != null) {
			Optional.ofNullable(headers.get("x-first-death-exchange"))
					.ifPresent(value -> this.setxFirstDeathExchange(value.toString()));
			Optional.ofNullable(headers.get("x-first-death-queue"))
					.ifPresent(value -> this.setxFirstDeathQueue(value.toString()));
			Optional.ofNullable(headers.get("x-first-death-reason"))
					.ifPresent(value -> this.setxFirstDeathReason(value.toString()));
			
			List<Map<String, Object>> xDeaths = (List<Map<String, Object>>) headers.get("x-death");
			
			Optional.ofNullable(xDeaths).
				ifPresent(list -> {
					list.stream().map(value -> {
						RabbitmqHeaderXDeath rabbitmqHeaderXDeath = new RabbitmqHeaderXDeath();
						Optional
							.ofNullable(value.get("reason"))
							.ifPresent(reason -> rabbitmqHeaderXDeath.setReason(reason.toString()));
						Optional
							.ofNullable(value.get("count"))
							.ifPresent(count -> rabbitmqHeaderXDeath.setCount(Integer.parseInt(count.toString())));
						Optional
							.ofNullable(value.get("exchange"))
							.ifPresent(exchange -> rabbitmqHeaderXDeath.setExchange(exchange.toString()));
						Optional
							.ofNullable(value.get("queue"))
							.ifPresent(queue -> rabbitmqHeaderXDeath.setQueue(queue.toString()));
						Optional
							.ofNullable(value.get("time"))
							.ifPresent(time -> rabbitmqHeaderXDeath.setTime((Date) time));

						Optional
							.ofNullable(value.get("routing-keys"))
							.ifPresent(routingKeys -> rabbitmqHeaderXDeath.setRoutingKeys((List<String>) routingKeys));
						
						this.xDeaths.add(rabbitmqHeaderXDeath);
						return value;
					}
					);
				}
						
			);
		}
	}
	
	public long getFailedRetryCount() {
		return 
			this.getxDeaths()
				.stream()
				.map(xDeath -> xDeath)
				.filter(xDeath -> xDeath.getQueue().toLowerCase().endsWith(KEYWORD_QUEUE_WAIT) && xDeath.getExchange().toLowerCase().endsWith(KEYWORD_QUEUE_WAIT))
				.count();
	}

	public List<RabbitmqHeaderXDeath> getxDeaths() {
		return xDeaths;
	}

	public String getxFirstDeathExchange() {
		return xFirstDeathExchange;
	}

	public String getxFirstDeathQueue() {
		return xFirstDeathQueue;
	}

	public String getxFirstDeathReason() {
		return xFirstDeathReason;
	}

	public void setxDeaths(List<RabbitmqHeaderXDeath> xDeaths) {
		this.xDeaths = xDeaths;
	}

	public void setxFirstDeathExchange(String xFirstDeathExchange) {
		this.xFirstDeathExchange = xFirstDeathExchange;
	}

	public void setxFirstDeathQueue(String xFirstDeathQueue) {
		this.xFirstDeathQueue = xFirstDeathQueue;
	}

	public void setxFirstDeathReason(String xFirstDeathReason) {
		this.xFirstDeathReason = xFirstDeathReason;
	}
}
