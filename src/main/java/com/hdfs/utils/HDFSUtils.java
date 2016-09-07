package com.hdfs.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 *  hadoop fs -chmod 777 /user/hadoop
 * @author Neusoft
 * <property>
      <name>dfs.permissions</name>
       <value>false</value>
</property>
 *
 */
public class HDFSUtils {
	private static final Logger log = Logger.getLogger(HDFSUtils.class);

	/*** 
     * 加载配置文件 
     * **/  
    private static Configuration conf = new Configuration();  
  
    private static FileSystem fs = null;  
  
    static {  
        try {  
//            conf.set("fs.defaultFS", "hdfs://hadoop-1"); 
//            conf.set("fs.defaultFS", "hdfs://hadoop2"); 
//            conf.set("dfs.nameservices", "hadoop2cluster");
//            conf.set("dfs.ha.namenodes.hadoop2cluster", "nn1,nn2");
//            conf.set("dfs.namenode.rpc-address.hadoop2cluster.nn1", "hadoop1:8020");
//            conf.set("dfs.namenode.rpc-address.hadoop2cluster.nn2", "hadoop2:8020");
//            conf.set("dfs.client.failover.proxy.provider.hadoop2cluster","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//            conf.setInt("dfs.replication", 2);  
            // 加载配置项  
        	System.setProperty("HADOOP_USER_NAME","root");
            fs = FileSystem.get(conf);  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }
    
    
    public static void main(String[] args) throws Exception {  
    	String path="/wordCount/inputData";
//    	createDirOnHDFS(path);  //创建文件夹
//		createFileOnHDFS("/path/file", "context");  //创建文件
//        uploadFile("E:\\20160718",path);  //上传文件
        deleteFileOrDirOnHDFS("/wordCount/outputData");  //删除文件
//        renameFileOrDirOnHDFS("/auditlog/inputLog", "/path");  //重命名文件
//        readFile("/auditlog/inputLog/20160701-10.81.88.103.txt");  //读取文件
//        readAllFile("/auditlog/inputLog");  //读取所有文件
//        downloadFileorDirectoryOnHDFS("/auditlog/inputLog/20160701-10.81.88.103.txt", "F:\\20160701-10.81.88.103.txt");  //下载文件
    }  
    
    //文件是否存在,假设存在则删除
    public static void FileExistDelete(Path path){
    	try {
			if(fs.exists(path)){
				fs.delete(path, true);
				log.info(path.getName()+"文件删除成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    /**
	 * 读取文件夹内的文件
	 */
	public static  File[] readFilePath(String filepath) throws IOException {
		File[] filelist = {};
		File file = new File(filepath);
		if (file.exists()) {
			if (file.isDirectory()) {
				filelist = file.listFiles();
				if (filelist.length > 0) {
					return filelist;
				} else {
					log.info(filepath + " 没有文件");
				}
			}
		} else {
			log.info(filepath + " 文件夹不存在");
		}
		return filelist;
	}
  
    /*** 
     * 上传本地文件到 HDFS上 
     *  
     * **/  
    public static void uploadFile(String src, String dst) throws Exception {  
    	File[] file = readFilePath(src);
		long start = System.currentTimeMillis();
		for (int i = 0; i < file.length; i++) {
			// 本地文件路径  
			Path srcPath = new Path(file[i].toString());  
			// HDFS文件路径  
			Path dstPath = new Path(dst);  
			try {  
				// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false  
				fs.copyFromLocalFile(false, srcPath, dstPath);  
			} catch (IOException e) {  
				log.error(e.getMessage());
			}  
			log.info("上传成功:"+file[i].getName()+" -- "+(i+1)+"个文件");  
		}
		fs.close();// 释放资源  
		long end = System.currentTimeMillis();
		log.info("需要时间为：" +((end - start)/1000)+"秒");
		
    }  
  
    /** 
     * 在HDFS上创建一个文件夹 
     *  
     * **/  
    public static void createDirOnHDFS(String path) throws Exception {  
        Path p = new Path(path);  
        boolean done = fs.mkdirs(p);  
        if (done) {  
            log.info("创建文件夹成功!");  
        } else {  
            log.info("创建文件夹成功!");  
        }  
//        fs.close();// 释放资源  
    }  
  
    /** 
     * 在HDFS上创建一个文件 
     *  
     * **/  
    public static void createFileOnHDFS(String dst, String content) throws Exception {  
        Path dstPath = new Path(dst);  
        // 打开一个输出流  
        FSDataOutputStream outputStream = fs.create(dstPath);  
        outputStream.write(content.getBytes());  
        outputStream.close();  
        fs.close();// 释放资源  
        log.info("创建文件成功!");  
  
    }  
  
    /** 
     * 在HDFS上删除一个文件或文件夹 
     *  
     * **/  
    public static void deleteFileOrDirOnHDFS(String path) throws Exception {  
        Path p = new Path(path);  
        boolean done = fs.deleteOnExit(p);  
        if (done) {  
            log.info("删除成功！");  
        } else {  
            log.info("删除失败！");  
        }  
        fs.close();// 释放资源  
  
    }  
  
    /** 
     * 重名名一个文件夹或者文件 
     *  
     * **/  
    public static void renameFileOrDirOnHDFS(String oldName, String newName) throws Exception {  
        Path oldPath = new Path(oldName);  
        Path newPath = new Path(newName);  
        boolean done = fs.rename(oldPath, newPath);  
        if (done) {  
            log.info("重命名文件夹或文件成功！");  
        } else {  
            log.info("重命名文件夹或文件失败！");  
        }  
        fs.close();// 释放资源  
    }  
  
    /*** 
     *  
     * 读取HDFS中某个文件 
     *  
     * **/  
    public static void readFile(String filePath) throws IOException {  
        Path srcPath = new Path(filePath);  
        InputStream in = null;  
        try {  
            in = fs.open(srcPath);  
            IOUtils.copyBytes(in, System.out, 4096, false); // 复制到标准输出流  
        } finally {  
            IOUtils.closeStream(in);  
        }  
    }  
  
    /*** 
     *  
     * 读取HDFS某个文件夹的所有 文件，并打印 
     *  
     * **/  
    public static List<Path> readAllFile(String path) {  
        // 打印文件路径下的所有文件名  
    	List<Path> list=new ArrayList<Path>();
        Path dstPath = new Path(path);  
        FileStatus[] fileStatus = null;  
        try {  
            fileStatus = fs.listStatus(dstPath);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        for (FileStatus status : fileStatus) {  
            if (status.isDirectory()) {  
                readAllFile(status.getPath().toString());  
            } else {  
                log.info("文件: " + status.getPath());  
                list.add(status.getPath());
//                try {  
//                    readFile(status.getPath().toString());  
//                } catch (IOException e) {  
//                    e.printStackTrace();  
//                }  
            }  
        }
		return list;  
    }  
  
    /** 
     * 从HDFS上下载文件或文件夹到本地 
     *  
     * **/  
    public static void downloadFileorDirectoryOnHDFS(String src, String dst) throws Exception {  
        Path srcPath = new Path(src);  
        Path dstPath = new Path(dst);  
        fs.copyToLocalFile(false, srcPath, dstPath);  
        fs.close();// 释放资源  
        log.info("下载文件夹或文件成功!");  
    }  

}
