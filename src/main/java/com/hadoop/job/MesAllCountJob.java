package com.hadoop.job;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.hadoop.mapper.MesCountMapper;
import com.hadoop.util.ScanUtil;

/**
 * 统计所有的有效的异常数据
 * @ClassName: MesAllCountJob
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author ZJHao
 * @date 2015-8-28 下午4:26:33
 *
 */
public class MesAllCountJob {

	private static String tableName="mesReq";
	public static void runJob(String sysCode) throws Exception{
		 Configuration conf = new Configuration();      
         conf.set("hbase.zookeeper.quorum", "10.6.2.56:2181,10.6.2.57:2181,10.6.2.58:2181");    
        
		long startTime=System.currentTimeMillis();
//		HTable htb=new HTable(conf, tableName);
		
		
		Job job=new Job(conf,"messageStatistics  job");
		job.setJarByClass(MesAllCountJob.class);
		
//		Scan scan=new ScanBase().findSysMess("01");
		System.out.println("sysCode:"+sysCode);
		Scan scan=new ScanUtil().findSysMessScan();
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MesCountMapper.class, Text.class, Text.class, job);
//		TableMapReduceUtil.initTableReducerJob(tableName,MesAllCodeReduceHbase.class , job);
		job.setMapOutputValueClass(Text.class);
		
		System.out.println("takes1: "+(System.currentTimeMillis()-startTime));
		
		job.waitForCompletion(true);
		System.out.println("takes: "+(System.currentTimeMillis()-startTime));
		
//		MesSysProcessReq sysReq=new MessageDaoImpl().selectAll(sysCode);
//		System.out.println(JSONObject.toJSONString(sysReq));
	}
	public static void main(String[] args) {
		try {
			runJob("04");
		} catch (Exception e) {
			System.out.println("执行runJob失败");
		}
	}
	
}
