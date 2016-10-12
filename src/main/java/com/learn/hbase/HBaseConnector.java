package com.learn.hbase;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConnector {

	public static void main(String[] args) throws IOException {

		Date date = new Date();

		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these
		// can be found on the CLASSPATH
		Configuration conf = HBaseConfiguration.create();

		// Instance to create a connection using HBase configuration.
		Connection conn = ConnectionFactory.createConnection(conf);

		// This instantiates an TableName object that connects you to the
		// "mylittlehbasetable" table.
		Table table = conn.getTable(TableName.valueOf(("mylittlehbasetable")));

		// To add to a row, use Put. A Put constructor takes the name of the row
		// you want to insert into as a byte array. In HBase, the Bytes class
		// has utility for converting all kinds of java types to byte arrays. In
		// the below, we are converting the String "myLittleRow" into a byte
		// array to use as a row key for our update. Once you have a Put
		// instance, you can adorn it by setting the names of columns you want
		// to update on the row, the timestamp to use in your update, etc.
		// If no timestamp, the server applies current time to the edits.
		Put put = new Put(Bytes.toBytes("mylittlerow"));

		put.addColumn(Bytes.toBytes("mylittlehbasefamily"),
				Bytes.toBytes("somequalifier"),
				Bytes.toBytes("Some value-" + date.toString()));

		// Once you've adorned your Put instance with all the updates you want
		// to make, to commit it do the following
		// (The Table#put method takes the Put instance you've been building
		// and pushes the changes you made into hbase)
		table.put(put);

		// Now, to retrieve the data we just wrote. The values that come back
		// are Result instances. Generally, a Result is an object that will
		// package up the hbase return into the form you find most palatable.
		Get get = new Get(Bytes.toBytes(("mylittlerow")));

		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("mylittlehbasefamily"),
				Bytes.toBytes("somequalifier"));

		// If we convert the value bytes, we should get back 'Some Value', the
		// value we inserted at this location.
		String valueStr = Bytes.toString(value);

		System.out.println("GET :" + valueStr);

		// Sometimes, you won't know the row you're looking for. In this case,
		// you use a Scanner. This will give you cursor-like interface to the
		// contents of the table. To set up a Scanner, do like you did above
		// making a Put and a Get, create a Scan. Adorn it with column names,
		// etc.

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("mylittlehbasefamily"),
				Bytes.toBytes("somequalifier"));
		ResultScanner resultScanner = null;
		
		try {
			resultScanner = table.getScanner(scan);

			for (Result row : resultScanner) {
				System.out.println("Found row: " + row);
			}

		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			resultScanner.close();
		}

	}
}
