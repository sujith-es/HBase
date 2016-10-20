package com.learn.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class HBaseTemperatureImporter extends Configured implements Tool {

	static class HBaseTemperatureMapper<K> extends
			Mapper<LongWritable, Text, K, Put> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
	
		}

	}

	public static void main(String[] args) {

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
