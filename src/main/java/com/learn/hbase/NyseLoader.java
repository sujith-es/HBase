package com.learn.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class NyseLoader {

	//Data for this program in NYSE dataset.
	final static Configuration conf = HBaseConfiguration.create();
	static NyseParser nyseParse = new NyseParser();
	static Table table;
	static final Logger logger = Logger.getLogger(NyseLoader.class);

	public static void main(String[] args) throws IOException {
		Connection connection = null;

		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("nyse:stock_data"));

			readFilesAndLoad(table, args[0]);

		} catch (Exception ex) {
			System.out.println("error in main() " + ex.toString());
		} finally {
			table.close();
			connection.close();
		}

	}

	private static void readFilesAndLoad(Table table, String nysePath) {

		List<Put> puts = new ArrayList<Put>();

		File localInputFolder = new File(nysePath);
		File[] files = localInputFolder.listFiles();

		for (File file : files) {
			BufferedReader br = null;
			if (file.getName().endsWith("csv")) {
				try {
					String sCurrentLine;
					br = new BufferedReader(new FileReader(file));

					while ((sCurrentLine = br.readLine()) != null) {
						nyseParse.parse(sCurrentLine);
						puts.add(buildPutList(table, nyseParse));
					}
					loadPutList(puts, table);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

	}

	private static void loadPutList(List<Put> puts, Table table)
			throws IOException {
		table.put(puts);

	}

	private static Put buildPutList(Table table2, NyseParser nyseRecord) {

		final String family = "sd";
		final String op = "op";
		final String hp = "hp";
		final String lp = "lp";
		final String cp = "cp";
		final String volume = "volume";

		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		String transactionDate = null;
		try {
			transactionDate = (new SimpleDateFormat("yyyy-MM-dd")
					.format(formatter.parse(nyseRecord.getTransactionDate())));
		} catch (Exception e) {
			System.out.println("error in buildPutList(): " + e.toString());
		}

		if (transactionDate == null || transactionDate.equals("null"))
			System.out.println(nyseRecord.getTransactionDate());

		Put put = new Put(Bytes.toBytes(nyseRecord.getStockTicker() + ","
				+ nyseRecord.getTransactionDate()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(op),
				Bytes.toBytes(nyseParse.getOpenPrice().floatValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(hp),
				Bytes.toBytes(nyseParse.getHighPrice().floatValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(lp),
				Bytes.toBytes(nyseParse.getLowPrice().floatValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(cp),
				Bytes.toBytes(nyseParse.getClosePrice().floatValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(volume),
				Bytes.toBytes(nyseParse.getVolume().intValue()));

		return put;

	}
}
