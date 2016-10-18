package com.learn.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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

public class ExampleClient {

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {

		// Create HBaseConfiguration.Assign it to
		Configuration Config = HBaseConfiguration.create();

		// Create connection object. Provides access to configuration
		// parameters.
		Connection conn = ConnectionFactory.createConnection(Config);

		/**
		 * The administrative API for HBase. Admin can be used to create, drop,
		 * // list, enable and disable tables, add and drop table column
		 * families and other administrative operations. Mainly for
		 * administering HBase cluster
		 */
		Admin admin = conn.getAdmin();

		// Get TableName object reference to access specific table
		TableName tableName = TableName.valueOf("test");

		// Check if table already exists. If exists,disable first and
		// delete.
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		/**
		 * HTableDescriptor contains the details about an HBase table such as
		 * the descriptors of all the column families, is the table a catalog
		 * table
		 */
		HTableDescriptor htd = new HTableDescriptor(tableName);

		/**
		 * An HColumnDescriptor contains information about a column family such
		 * as the number of versions, compression settings, etc.
		 *
		 * It is used as input when creating a table or adding a column.
		 */
		HColumnDescriptor hcd = new HColumnDescriptor("data");

		// Add columnDescriptor to TableDescriptor object.
		htd.addFamily(hcd);

		// Create table using Admin object, pass the complete assigned
		// HTableDescriptor object.
		admin.createTable(htd);

		// Get all the table from current configured DB. stored in as Array
		// of HTableDescriptor.
		HTableDescriptor[] tables = admin.listTables();

		// If current tableName is not found, then table was not created.
		// Throw exception.
		if (tables.length != 1
				&& Bytes.equals(tableName.getName(), tables[0].getTableName()
						.getName())) {
			throw new IOException("faile to create table.");
		}

		// Loop through and insert data using Put.
		Table table = conn.getTable(tableName);

		try {
			for (int i = 0; i <= 3; i++) {
				byte[] row = Bytes.toBytes("row" + i);
				// new row reference
				Put put = new Put(row);
				// add family name
				byte[] columnFamily = Bytes.toBytes("data");
				// Add qualifier name
				byte[] qualifier = Bytes.toBytes(String.valueOf(i));

				// value of column
				byte[] value = Bytes.toBytes("value" + i);

				// Add the specified column and value to this Put operation.
				put.addColumn(columnFamily, qualifier, value);

				/**
				 * Don't forget below line, Put operation has to be added to
				 * Table Put method.
				 */
				table.put(put);
			}

			// Get operation to read row1 inserted above in Put operation.
			Get get = new Get(Bytes.toBytes("row1"));

			// Read table row and assign to Result object. Single row result of
			// a Get operation.
			Result result = table.get(get);

			System.out.println("Get: " + result);

			// Traditional database like cursor. Create scan object.
			Scan scan = new Scan();

			// Returns row in order for table defined above (table "test")
			ResultScanner resultScanner = table.getScanner(scan);

			try {
				// loop through scan object returned
				for (Result scanResult : resultScanner) {
					System.out.println("scan:" + scanResult);
				}
			} finally {
				resultScanner.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			table.close();
			admin.close();
		}

	}
}
