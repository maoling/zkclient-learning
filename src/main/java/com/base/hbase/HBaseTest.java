//package com.base.hbase;
//
//import javax.xml.transform.Result;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import com.base.enums.HbaseAdress;
//
//public class HBaseTest {
//	public static void main(String[] args) throws Exception {
//		HTable table = new HTable(getConfig(), TableName.valueOf("t1"));// 表名是test_table
//		Put put = new Put(Bytes.toBytes("row_03"));// 行键是row_01
//		put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("maoling"));// 列簇是f，列修饰符是name，值是Andy3
//		table.put(put);
//
//		/*
//		 * Get get = new Get(Bytes.toBytes("row_03"));
//		 * org.apache.hadoop.hbase.client.Result rest = table.get(get);
//		 * System.out.println(rest.toString());
//		 */
//		table.close();
//	}
//
//	public static Configuration getConfig() {
//		Configuration configuration = new Configuration();
//		// conf.set("hbase.rootdir","hdfs：HadoopMaster:9000/hbase");
//		configuration.set("hbase.zookeeper.quorum", HbaseAdress.HBASE_POS);
//		return configuration;
//	}
//}