package com.course.rabbitmqconsumer.entity;

public class Picture {

	private String name;
	private String type;
	private String source;
	private long size;
		
	public Picture() {
		super();
	}

	public Picture(String name, String type, String source, long size) {
		super();
		this.name = name;
		this.type = type;
		this.source = source;
		this.size = size;
	}

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public String getSource() {
		return source;
	}

	public String getType() {
		return type;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "Picture [name=" + name + ", type=" + type + ", source=" + source + ", size=" + size + "]";
	}

}
