package com.base.hbase;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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

public class HbaseDemo_0_98 {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    private static final String HBASE_POS = "172.17.0.2";//"192.168.99.100"; //
    // 初始化链接
    public static void init() {
    	 // 取得一个数据库连接的配置参数对象
    	configuration = HBaseConfiguration.create();

        // 设置连接参数：HBase数据库所在的主机IP
    	configuration.set("hbase.zookeeper.quorum", HBASE_POS);
        // 设置连接参数：HBase数据库使用的端口
    	configuration.set("hbase.zookeeper.property.clientPort", "2181");
        
    	configuration.set("hbase.master", HBASE_POS + ":60000");  
        // 取得一个数据库连接对象
        try {
			connection = ConnectionFactory.createConnection(configuration);
			// 取得一个数据库元数据操作对象
	        admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    // 关闭连接
    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 建表
    public static void createTable(String tableNmae, String[] cols) throws IOException {

        init();
        TableName tableName = TableName.valueOf(tableNmae);

        if (admin.tableExists(tableName)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    // 删表
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    // 查看已有表
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    // 插入数据
    public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val)
            throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        // 批量插入
        /*
         * List<Put> putList = new ArrayList<Put>(); puts.add(put);
         * table.put(putList);
         */
        table.close();
        close();
    }
    
    // 插入数据
    public static void insterRowByCellApi(String tableName, String rowkey, String colFamily, String col, String val)
            throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        
        /*Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);*/
        
		final CellBuilder cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put);
		for (int i = 0; i < 10; i++) {
			//cell.clear();			
			byte[] row = Bytes.toBytes("row_"+ i);
			Put put = new Put(row);
			cell.setRow(row);
			cell.setQualifier(col.getBytes());
			cell.setFamily(colFamily.getBytes());
			cell.setValue(val.getBytes());
			put.add(cell.build());
			table.put(put);
		}

        
        table.close();
        close();
    }
    
 // 插入数据
    public static void insterRowByCellApi2(String tableName, String rowkey, String colFamily, String col, String val)
            throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        
		for (int i = 0; i < 10; i++) {
			//cell.clear();			
			byte[] row = Bytes.toBytes("row_"+ i);
			Put put = new Put(row);
			CellBuilder cell = put.getCellBuilder();
			cell.setQualifier(col.getBytes());
			cell.setFamily(colFamily.getBytes());
			cell.setValue(val.getBytes());
			put.add(cell.build());
			table.put(put);
		}

        
        table.close();
        close();
    }

    // 删除数据
    public static void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // 删除指定列族
        // delete.addFamily(Bytes.toBytes(colFamily));
        // 删除指定列
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // 批量删除
        /*
         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);
         */
        table.close();
        close();
    }

    // 根据rowkey查找数据
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        // 获取指定列族数据
        // get.addFamily(Bytes.toBytes(colFamily));
        // 获取指定列数据
        // get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
    }

    // 格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    // 批量查找数据
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        // scan.setStartRow(Bytes.toBytes(startRow));
        // scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
        close();
    }
    
    public static void main(String[] args) throws IOException {
        //createTable("t1", new String[] { "cf1", "cf2" });
        //listTables();
    	//insterRow("t1", "rw1", "cf1", "q1", "val1"); 
    	insterRowByCellApi2("t1", "rw1", "cf1", "hometown", "yangzhou");
        //getData("t2", "rw1","cf1", "q1"); 
        /*        
        scanData("t2", "rw1", "rw2");
         * deleRow("t2","rw1","cf1","q1"); deleteTable("t2");
         */
    }

}