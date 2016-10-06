package com.learn.hbase;

public class NyseParser {

	String stockTicker;
	String transactionDate;
	Float openPrice;
	Float highPrice;
	Float lowPrice;
	Float closePrice;
	Integer volume;

	public void parse(String record) {
		// sample data format in NYSE data
		// NASDAQ,RMIX,08-02-2010,0.87,0.88,0.81,0.81,118100,0.81
		String[] vals = record.split(",");
		stockTicker = vals[1];
		transactionDate = vals[2];
		openPrice = new Float(vals[3]);
		highPrice = new Float(vals[4]);
		lowPrice = new Float(vals[5]);
		closePrice = new Float(vals[6]);
		volume = new Integer(vals[7]);
	}

	public String getStockTicker() {
		return stockTicker;
	}

	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}

	public String getTransactionDate() {
		return transactionDate;
	}

	public void setTransactionDate(String transactionDate) {
		this.transactionDate = transactionDate;
	}

	public Float getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(Float openPrice) {
		this.openPrice = openPrice;
	}

	public Float getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(Float highPrice) {
		this.highPrice = highPrice;
	}

	public Float getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(Float lowPrice) {
		this.lowPrice = lowPrice;
	}

	public Float getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(Float closePrice) {
		this.closePrice = closePrice;
	}

	public Integer getVolume() {
		return volume;
	}

	public void setVolume(Integer volume) {
		this.volume = volume;
	}

}
