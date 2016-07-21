package com.hadoop.job;

import com.hadoop.mapper.MesCountMapper;
import com.hadoop.mapper.MesCountReadFromHdfs;
import com.hadoop.reduce.MesAllCodeReduceUnprocess;
import com.hadoop.reduce.MesAllProcessUnProcessDbSourceRecordReduce;
import com.hadoop.util.HdfsDAO;
import com.hadoop.util.PropertiesLoad;
import com.hadoop.util.ScanUtil;
import com.hadoop.util.SystemBootstrap;
import com.hadoop.vo.MesAllProcessReq;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 统计所有的有效的异常数据
 * @ClassName: MesAllCountJob
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author ZJHao
 * @date 2015-8-28 下午4:26:33
 *
 */
public class MesAllCountUnprocessJob {



	public static String str=null;
	
	public static MesAllProcessReq mesAllProcessReq=null;
	
	public static void jobReduceHbase() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", PropertiesLoad.getProperties("exception.hadoop.job.zkServerUrl"));

		long startTime = System.currentTimeMillis();
		//		HTable htb=new HTable(conf, tableName);

		Job job = new Job(conf, "messageStatistics  job");
		job.setJarByClass(MesAllCountUnprocessJob.class);

		//		Scan scan=new ScanBase().findSysMess("01");
		Scan scan = new ScanUtil().findSysMessScanUnprocess();
		TableMapReduceUtil.initTableMapperJob(PropertiesLoad.getProperties("exception.hadoop.job.tableName"), scan, MesCountMapper.class, Text.class, Text.class, job);
				job.setReducerClass(MesAllCodeReduceUnprocess.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(PropertiesLoad.getProperties("exception.hadoop.job.hdfsServerUrl") + PropertiesLoad.getProperties("exception.hadoop.job.hdfsOutputPathUnprocess")));

				System.out.println("takes1: " + (System.currentTimeMillis() - startTime));

		job.waitForCompletion(true);
		System.out.println("takes: " + (System.currentTimeMillis() - startTime));

		//		MesSysProcessReq sysReq=new MessageDaoImpl().selectAll(sysCode);
		//		System.out.println(JSONObject.toJSONString(sysReq));
	}

	public static void jobInserMysql() {
		JobConf conf = new JobConf(MesAllCountUnprocessJob.class);
		// 设置输入输出类型

		
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);

		// 不加这两句，通不过，但是网上给的例子没有这两句。
		conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		// 设置Map和Reduce类

		conf.setMapperClass(MesCountReadFromHdfs.class);
		conf.setReducerClass(MesAllProcessUnProcessDbSourceRecordReduce.class);

		
		// 设置输入目录


		// 建立数据库连接

		DBConfiguration.configureDB(conf, PropertiesLoad.getProperties("exception.hadoop.job.jdbcDriver"),
//		"jdbc:mysql://10.6.2.50:3307/itmanager?useUnicode=true&characterEncoding=UTF8", "zt3307", "123456");
		PropertiesLoad.getProperties("exception.hadoop.job.jdbcUrl"), PropertiesLoad.getProperties("exception.hadoop.job.jdbcUserName"), PropertiesLoad.getProperties("exception.hadoop.job.jdbcPassword"));
//		"jdbc:mysql://172.16.3.202:3306/test?useUnicode=true&characterEncoding=UTF8", "root", "zhang");
		try {
			DistributedCache.addFileToClassPath(new Path(PropertiesLoad.getProperties("exception.hadoop.job.hdfsServerUrl")+"/lib/mysql-connector-java-5.0.4-bin.jar"), conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// 写入"wordcount"表中的数据

//		String[] fields = {  };
		FileInputFormat.setInputPaths(conf, new Path(PropertiesLoad.getProperties("exception.hadoop.job.hdfsServerUrl") + PropertiesLoad.getProperties("exception.hadoop.job.hdfsOutputPathUnprocess")));
				DBOutputFormat.setOutput(conf, "mes_all_process_req", "code", "warn_count", "error_count", "created_time", "process_status");
		
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void rmOutput(){
		JobConf conf = new JobConf(MesAllCountUnprocessJob.class);
		HdfsDAO hdfs = new HdfsDAO(PropertiesLoad.getProperties("exception.hadoop.job.hdfsServerUrl"), conf);
		try {
			String rmrPath=PropertiesLoad.getProperties("exception.hadoop.job.hdfsServerUrl")+PropertiesLoad.getProperties("exception.hadoop.job.hdfsOutputPathUnprocess");
			System.out.println(rmrPath);
			hdfs.rmr(rmrPath);
		} catch (IOException e) {
			System.out.println(PropertiesLoad.getProperties("exception.hadoop.job.hdfsOutputPathUnprocess")+"文件不存在");
		}
	}

	public static void main(String[] args) {
		try {
			SystemBootstrap.afterPropertiesSet();

			rmOutput();
			jobReduceHbase();
			jobInserMysql();
			System.out.println(MesAllCountUnprocessJob.str+"-------------------- str-----------main");
			System.out.println(MesAllCountUnprocessJob.mesAllProcessReq+"-------------------- mesAllProcessReq----------main");
			System.out.println(MesAllCountUnprocessJob.mesAllProcessReq.toString()+"-------------------- mesAllProcessReq.toString()----------main");
//			rmOutput();
		} catch (Exception e) {
			System.out.println(MesAllCountUnprocessJob.str+"-------------------- str");
			System.out.println("执行runJob失败");
		}
	}

}
