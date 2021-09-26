package io.muayed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil
{

	Configuration config;
	private static final String TABLE_NAME = "performance";
	private static final String CF_1 = "memory_usage";

	public HBaseUtil() {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "localhost:2183");
	}

	public void put(Performance performance)  {
		try (Connection connection = ConnectionFactory.createConnection(config)) {
			Table hTable = connection.getTable(TableName.valueOf(TABLE_NAME));
//			List<Put> puts = new ArrayList<>();

			Put p1 = new Put(Bytes.toBytes(performance.getTimestamp()));
			p1.addColumn(Bytes.toBytes(CF_1),
					Bytes.toBytes("currentMem"), Bytes.toBytes(performance.getCurrentMem().toString()));
			p1.addColumn(Bytes.toBytes(CF_1),
					Bytes.toBytes("totalMem"), Bytes.toBytes(performance.getTotalMem().toString()));
//			puts.add(p1);

			hTable.put(p1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createIfNotExist() {
		try (Connection connection = ConnectionFactory.createConnection(config);
			 Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_1).setCompressionType(Algorithm.NONE));
			System.out.print("Creating table.... ");
			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			System.out.println("Table Created!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}