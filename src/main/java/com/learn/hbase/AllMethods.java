package com.learn.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class AllMethods {

	private static Configuration conf = null;
	private static Connection conn = null;

	static {
		conf = HBaseConfiguration.create();
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void createTable(String table_Name, String[] familys)
			throws IOException {

		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf(table_Name);
		if (admin.tableExists(tableName)) {
			System.out.println("Table:" + tableName + " already exists.");
		} else {
			HTableDescriptor htd = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				htd.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(htd);
			System.out.println("Create table:" + table_Name + " Ok");
		}
	}

	private static void deleteTable(String table_Name) throws IOException {

		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf(table_Name);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		System.out.println("Delete table: " + table_Name + " Ok");
	}

	private static void addRecord(String table_Name, String rowKey,
			String family, String qualifier, String value) throws IOException {

		TableName tableName = TableName.valueOf(table_Name);
		Table table = conn.getTable(tableName);

		Put put = new Put(Bytes.toBytes(rowKey));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				Bytes.toBytes(value));

		table.put(put);
		System.out.println("Insert Record " + rowKey + " to table: "
				+ table_Name + " Ok");

	}

	private static void getOneRecord(String table_Name, String rowKey)
			throws IOException {

		TableName tableName = TableName.valueOf(table_Name);
		Get get = new Get(Bytes.toBytes(rowKey));

		Table table = conn.getTable(tableName);
		Result result = table.get(get);
		for (Cell rv : result.rawCells()) {
			System.out.println(Bytes.toString(rv.getRow()));
			System.out.println(new String(rv.getFamily()));
			System.out.println(new String(rv.getQualifier()));
			System.out.println(rv.getTimestamp());
			System.out.println(new String(rv.getValue()));
		}
	}

	private static void getAllRecords(String table_Name, String family,
			String qualifier) throws IOException {

		TableName tableName = TableName.valueOf(table_Name);

		Table table = conn.getTable(tableName);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		ResultScanner resultScanner = table.getScanner(scan);

		for (Result item : resultScanner) {
			for (Cell rv : item.rawCells()) {
				System.out.println(Bytes.toString(rv.getRow()));
				System.out.println(new String(rv.getFamily()));
				System.out.println(new String(rv.getQualifier()));
				System.out.println(rv.getTimestamp());
				System.out.println(new String(rv.getValue()));
			}
		}

	}

	public static void main(String[] args) throws IOException {

		final String tableName = "scores";
		String[] familys = { "grade", "course" };

		AllMethods.createTable(tableName, familys);

		// add record
		AllMethods.addRecord(tableName, "sujith", "grade", "", "A");
		AllMethods.addRecord(tableName, "sujith", "course", "", "90");
		AllMethods.addRecord(tableName, "sujith", "course", "math", "98");
		AllMethods.addRecord(tableName, "sujith", "course", "english", "98");

		// add record baoniu
		AllMethods.addRecord(tableName, "hima", "grade", "", "A");
		AllMethods.addRecord(tableName, "hima", "course", "math", "89");

		System.out.println("===========get one record========");
		AllMethods.getOneRecord(tableName, "sujith");

		System.out.println("===========show all record========");
		AllMethods.getAllRecords(tableName, "course", "");

		System.out.println("===========delete one record========");
		AllMethods.deleteTable(tableName);
		// AllMethods.getAllRecords(tableName);

		// System.out.println("===========show all record========");
		// AllMethods.getAllRecords(tableName);
	}
}
