package com.hbase.utils;




import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
/**
 * java.net.UnknownHostException: unknown host
 * 在C:\WINDOWS\system32\drivers\etc\hosts文件中添加如下信息：192.168.230.128 hbase
 * @author Neusoft
 *
 */
public class HbaseUtils {
	private static final Logger log=Logger.getLogger(HbaseUtils.class);
	
	static Configuration conf=null;
	static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
	static{
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
		//conf.set("hbase.zookeeper.property.clientPort", "2181"); 
	}
	
	public static void main(String[] args) throws Exception {
		// 创建表
        String tableName = "blog0";
        String[] family = { "article", "author" };
        createTable(tableName, family);

        
        // 为表添加数据
        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        putData( tableName,"rowkey1", column1, value1, column2, value2);
        putData(tableName,"rowkey2",  column1, value1, column2, value2);
        putData(tableName,"rowkey3",  column1, value1, column2, value2);
//        
        //根据表名查询所有数据
        queryAll(tableName);
        
        
        //根据rwokey查询
//        getResult(tableName,"rowkey1");
        
        
        //遍历查询hbase表
        //getResultScann(tableName);
        
        //条件遍历查询hbase
       // getResultScan(tableName, "rowkey1", "rowkey3");

        
        //查询表中的某一列的值
        //getResultByColumn(tableName, "rowkey1", "article", "title");
        
        
        
        // 更新表中的某一列
        /**改变value:修改value,不新增
         * 改变列字段名称：新增一条
         * 改变rowKey:新增一条
         */
        //updateTable(tableName, "rowkey1", "article", "title", "Head First HBase-----45454545454");

        
        //查询某列数据的多个版本
        //getResultByVersion(tableName, "rowkey1", "article", "title");
        
        
        //删除指定的列
        //deleteColumn(tableName, "rowkey3", "article", "content");
		
        
       //删除指定的列
        //deleteAllColumn(tableName, "rowkey3");
        
        
        //删除表
        //deleteTable(tableName);
        
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param ColumnFamily
	 */
	public static void createTable(String tableName,String[] ColumnFamily){
		HBaseAdmin admin=null;
		try {
			admin=new HBaseAdmin(conf);
			log.info(admin.toString());
			if(admin.tableExists(TableName.valueOf(tableName))){
				log.info(tableName + " is exist,detele....ing");  
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
//				System.exit(0);
			}
			HTableDescriptor desc=new HTableDescriptor(TableName.valueOf(tableName));
			for(int i=0;i<ColumnFamily.length;i++){
				desc.addFamily(new HColumnDescriptor(ColumnFamily[i]));//添加列族
				desc.setMemStoreFlushSize(1024*1024*1024);
				desc.setDurability(Durability.USE_DEFAULT);
				/**
				 *  Durability取值:
					ASYNC_WAL ： 当数据变动时，异步写WAL日志
					SYNC_WAL ： 当数据变动时，同步写WAL日志
					FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
					SKIP_WAL ： 不写WAL日志
					USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即 SYNC_WAL
				 */
			}
				admin.createTable(desc);
				log.info("create table Success!");
		} catch (Exception e) {
			e.printStackTrace();
			log.info("error!");
		}
	}
	
	/**
	 * 添加数据
	 * @param tableName
	 * @param rowKey
	 * @param column1
	 * @param value1
	 * @param column2
	 * @param value2
	 * @throws Exception
	 */
	public static void putData(String tableName,String rowKey,
            String[] column1, String[] value1, String[] column2, String[] value2) throws Exception{
		Put put=new Put(Bytes.toBytes(rowKey));// 设置rowkey
		HTable table=new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
		HColumnDescriptor[] columnDescriptors=table.getTableDescriptor().getColumnFamilies();// 获取所有的列族
		for(int i=0;i<columnDescriptors.length;i++){
			String familyName=columnDescriptors[i].getNameAsString();//获取列族名
			if(familyName.equals("article")){// article列族put数据
				for(int j=0;j<column1.length;j++){
					put.add(Bytes.toBytes(familyName),Bytes.toBytes(column1[j]),Bytes.toBytes(value1[j]));
				}
			}
			if(familyName.equals("author")){
				for(int j=0;j<column2.length;j++){
					put.add(Bytes.toBytes(familyName),Bytes.toBytes(column2[j]),Bytes.toBytes(value2[j]));
				}
			}
		}
		table.put(put);
		table.close();
		log.info("add data success!");
	}
	
	/**
	 * 根据表名查询
	 * @param tableName
	 * @throws IOException 
	 */
	public static void queryAll(String tableName) throws IOException{
		HTable table=new HTable(conf, tableName);
		ResultScanner  rs=table.getScanner(new Scan());
		log.info("-------------------------------------------");
		for(Result r:rs){
			for(Cell kv:r.rawCells()){
				log.info("rowKey:"+Bytes.toString(kv.getRowArray()));
				log.info("family:"+Bytes.toString(kv.getFamilyArray()));
				log.info("qualifier:"+Bytes.toString(kv.getQualifierArray()));
				log.info("value:"+Bytes.toString(kv.getValueArray()));
				log.info("Timestamp:" + sdf.format(new Date(kv.getTimestamp())));
		        log.info("-------------------------------------------");
			}
		}
		table.close();
	}
	
	
	 /*
     * 根据rwokey查询
     * @rowKey rowKey
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey)throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        log.info("-------------------------------------------");
        for (Cell kv : result.listCells()) {
        	log.info("rowName(行键):"+new String(CellUtil.cloneRow(kv)));
        	log.info("family(列族):"+new String(CellUtil.cloneFamily(kv)));
        	log.info("qualifier(列):"+new String(CellUtil.cloneQualifier(kv)));
        	log.info("value(值):"+new String(CellUtil.cloneValue(kv)));
        	
//        	log.info("rowKey:"+Bytes.toString(kv.getRowArray()));
//            log.info("family:" + Bytes.toString(kv.getFamilyArray()));
//            log.info("qualifier:" + Bytes.toString(kv.getQualifierArray()));
//            log.info("value:" + Bytes.toString(kv.getValueArray()));
            log.info("Timestamp:" + sdf.format(new Date(kv.getTimestamp())));
            log.info("-------------------------------------------");
        }
        table.close();
        return result;
    }
	
	/**
	 * 遍历hbase表
	 * @throws IOException 
	 */
	public static void getResultScann(String tableName) throws IOException{
		Scan scan=new Scan();
		ResultScanner rs=null;
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		try {
			rs=table.getScanner(scan);
			for(Result r:rs){
				for(Cell kv:r.listCells()){
					log.info("tableName:"+tableName);
					log.info("rowKey:" + Bytes.toString(kv.getRowArray()));
			        log.info("family:" + Bytes.toString(kv.getFamilyArray()));
			        log.info("qualifier:" + Bytes.toString(kv.getQualifierArray()));
			        log.info("value:" + Bytes.toString(kv.getValueArray()));
			        log.info("timestamp:" + kv.getTimestamp());
			        log.info("-------------------------------------------");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			rs.close();
			table.close();
		}
	}
	
	/**
	 * 遍历查询hbase
	 * @param tableName
	 * @param start_rowkey
	 * @throws IOException 
	 */
	public static void getResultScan(String tableName,String start_rowkey,String stop_rowkey) throws IOException{
		Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell kv : r.listCells()) {
                    log.info("row:" + Bytes.toString(kv.getRowArray()));
                    log.info("family:" + Bytes.toString(kv.getFamilyArray()));
                    log.info("qualifier:" + Bytes.toString(kv.getQualifierArray()));
                    System.out .println("value:" + Bytes.toString(kv.getValueArray()));
                    log.info("timestamp:" + kv.getTimestamp());
                    System.out .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
            table.close();
        }
	}
	
	/**
	 * 查询表中某一列
	 * @param tableName
	 * @param rowKey
	 * @param familyName
	 * @param columnName
	 * @throws IOException
	 */
	public static void getResultByColumn(String tableName,String rowKey,String familyName,String columnName) throws IOException{
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		Get get=new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));// 获取指定列族和列修饰符对应的列
		Result result=table.get(get);
		for(Cell kv:result.listCells()){
			log.info("family:"+Bytes.toString(kv.getFamilyArray()));
			 log.info("qualifier:" + Bytes.toString(kv.getQualifierArray()));
			 log.info("value:" + Bytes.toString(kv.getValueArray()));
			 log.info("Timestamp:" + kv.getTimestamp());
			 log.info("-------------------------------------------");
		}
		table.close();
	}
	
	/*
     * 更新表中的某一列
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列族名
     * @columnName 列名
     * @value 更新后的值
     */
	public static void updateTable(String tableName,String rowKey,String familyName,String columnName,String value) throws IOException{
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		Put put=new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
		table.put(put);
		table.close();
		log.info("update table Success!");
	}
	
	/*
     * 查询某列数据的多个版本
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列族名
     * @columnName 列名
     */
	public static void getResultByVersion(String tableName,String rowKey,String familyName,String columnName) throws IOException{
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		Get get=new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		get.setMaxVersions(5);
		Result result=table.get(get);
		for(Cell kv:result.listCells()){
			log.info("rowKey:" + Bytes.toString(kv.getRowArray()));
			log.info("family:" + Bytes.toString(kv.getFamilyArray()));
            log.info("qualifier:" + Bytes.toString(kv.getQualifierArray()));
            log.info("value:" + Bytes.toString(kv.getValueArray()));
            log.info("Timestamp:" + kv.getTimestamp());
            log.info("-------------------------------------------");
		}
		table.close();
		/*
         * List<?> results = table.get(get).list(); 
         * Iterator<?> it =results.iterator();
         * while (it.hasNext()) {
         * log.info(it.next().toString()); 
         * }
         */
	}
	
	
	/*
     * 删除指定的列
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列族名
     * @columnName 列名
     */
	public static void deleteColumn(String tableName,String rowKey,String familyName,String columnName) throws IOException{
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteColumn=new Delete(Bytes.toBytes(rowKey));
		deleteColumn.deleteColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
		table.delete(deleteColumn);
		table.close();
		log.info(familyName + ":" + columnName + "is deleted!");
	}
	
	
	/*
     * 删除指定的列
     * @tableName 表名
     * @rowKey rowKey
     */
	public static void deleteAllColumn(String tableName,String rowKey) throws Exception{
		HTable table=new HTable(conf, Bytes.toBytes(tableName));
		Delete deleteAll=new Delete(Bytes.toBytes(rowKey));
		table.delete(deleteAll);
		log.info(rowKey+" all columns are deleted!");
		table.close();
	}
	
	 /*
     * 删除表
     * @tableName 表名
     */
	public static void deleteTable(String tableName) throws IOException{
		HBaseAdmin admin=new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		log.info(tableName + "is deleted!");
		admin.close();
	}
}
