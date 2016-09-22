package org.joolzminer.examples.spark.java;

import java.io.Serializable;
import java.sql.Timestamp;

@SuppressWarnings("serial")
public class Order implements Serializable {
	private Timestamp time;
	private Long orderId;
	private Long clientId;
	private String symbol;
	private Integer amount;
	private Double price;
	private Boolean isBuy;
	
	public Order(Timestamp time, Long orderId, Long clientId, String symbol, Integer amount, Double price, Boolean isBuy) {
		this.time = time;
		this.orderId = orderId;
		this.clientId = clientId;
		this.symbol = symbol;
		this.amount = amount;
		this.price = price;
		this.isBuy = isBuy;
	}

	public Timestamp getTime() {
		return time;
	}

	public Long getOrderId() {
		return orderId;
	}

	public Long getClientId() {
		return clientId;
	}

	public String getSymbol() {
		return symbol;
	}

	public Integer getAmount() {
		return amount;
	}

	public Double getPrice() {
		return price;
	}

	public Boolean isBuy() {
		return isBuy;
	}

	@Override
	public String toString() {
		return "Order [time=" + time + ", orderId=" + orderId + ", clientId=" + clientId + ", symbol=" + symbol
				+ ", amount=" + amount + ", price=" + price + ", isBuy=" + isBuy + "]";
	}
}
