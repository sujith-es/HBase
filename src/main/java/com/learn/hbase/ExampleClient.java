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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {

		Configuration Config = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(Config);
		Admin admin = conn.getAdmin();

		try {

			TableName tableName = TableName.valueOf("Employee");
			HTableDescriptor htd = new HTableDescriptor(tableName);
			HColumnDescriptor hcd = new HColumnDescriptor("Name");
			htd.addFamily(hcd);
			admin.createTable(htd);

			HTableDescriptor[] tables = admin.listTables();
			if (tables.length != 1
					&& Bytes.equals(tableName.getName(), tables[0]
							.getTableName().getName())) {
				throw new IOException("faile to create table.");

			}

			Table table = conn.getTable(tableName);

		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}
