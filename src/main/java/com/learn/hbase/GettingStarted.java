package com.learn.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GettingStarted {

	static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws IOException {

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("demo"));

		Scan scan = new Scan();
		ResultScanner resultScan = table.getScanner(scan);

		try {
			//
			// for (Result res : resultScan) {
			// System.out.println(Bytes.toString(res.getRow()));
			// System.out.println(Bytes.toString(res.getValue(
			// "cf1".getBytes(), "column1".getBytes())));
			// System.out.println(Bytes.toString(res.getValue(
			// "cf1".getBytes(), "column2".getBytes())));
			//
			// }

			// Put put = new Put("3".getBytes());
			//
			// put.addColumn("cf1".getBytes(), "column1".getBytes(),
			// "value1".getBytes());
			//
			// put.addColumn("cf1".getBytes(), "column2".getBytes(),
			// "value2".getBytes());

			// table.put(put);

			Get get = new Get("3".getBytes());
			Result result = table.get(get);
			System.out.println(Bytes.toString(result.getValue("cf1".getBytes(),
					"column1".getBytes())));
			System.out.println(Bytes.toString(result.getValue("cf1".getBytes(),
					"column2".getBytes())));

			// Delete delete = new Delete("3".getBytes());
			// table.delete(delete);

		} catch (Exception e) {
			System.out.println(e.toString());
		} finally {
			resultScan.close();
		}

	}
}
