package com.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 获取output文件中的reduce后的MesAllCountReq数据
 * @ClassName: MesCountReadFromHdfs
 * @author ZJHao
 * @date 2015-10-1 下午3:02:15
 *
 */
public class MesCountReadFromHdfs extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>{

	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		String val[] =value.toString().split("\t");
		output.collect(key,new Text(value.toString()));
	}


}
