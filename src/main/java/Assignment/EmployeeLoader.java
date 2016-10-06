package Assignment;

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

import com.learn.hbase.NyseLoader;
import com.learn.hbase.NyseParser;

public class EmployeeLoader {
	
	final static Configuration conf = HBaseConfiguration.create();
	static EmployeeParser empParser = new EmployeeParser();
	static Table table;
	static final Logger logger = Logger.getLogger(EmployeeLoader.class);

	public static void main(String[] args) throws IOException {
		Connection connection = null;

		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("google:employee"));

			readFilesAndLoad(table, args[0]);

		} catch (Exception ex) {
			System.out.println("error in main() " + ex.toString());
		} finally {
			table.close();
			connection.close();
		}

	}

	private static void readFilesAndLoad(Table table, String empFilePath) {

		List<Put> puts = new ArrayList<Put>();

		File localInputFolder = new File(empFilePath);
		File[] files = localInputFolder.listFiles();

		for (File file : files) {
			BufferedReader br = null;
			if (file.getName().endsWith("csv")) {
				try {
					String sCurrentLine;
					br = new BufferedReader(new FileReader(file));

					while ((sCurrentLine = br.readLine()) != null) {
						empParser.parse(sCurrentLine);
						puts.add(buildPutList(empParser));
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

	private static Put buildPutList(EmployeeParser empRecord) {

		final String family = "emp";
		final String FullName = "Name";
		final String laneName = "Lane";
		final String zip = "Zip";
		final String area = "Area";
		final String salary = "Salary";

		Put put = new Put(Bytes.toBytes(empRecord.getEmpId().intValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(FullName),
				Bytes.toBytes(empParser.getFullName()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(salary),
				Bytes.toBytes(empParser.getSalary().floatValue()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(laneName),
				Bytes.toBytes(empParser.getLaneName()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(zip),
				Bytes.toBytes(empParser.getZipCode()));

		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(area),
				Bytes.toBytes(empParser.getAreaName()));

		return put;

	}

}
