package com.mine.jedistest;

import java.io.Serializable;

public class ImpressionVO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6822823256698410651L;
	private String id;
	private Long cost;
	
	public String toString() {
		return "id:"+id + " | cost:"  + cost;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Long getCost() {
		return cost;
	}
	public void setCost(Long cost) {
		this.cost = cost;
	}
}
