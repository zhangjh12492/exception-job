package com.hadoop.mapper;

import com.hadoop.util.StringUtils;
import com.hadoop.vo.ClientExceptionReq;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * 获取hbase数据的mappper
 * @ClassName: MesCountMapper
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author ZJHao
 * @date 2015-10-1 下午3:01:58
 *
 */
public class MesCountMapper extends TableMapper<Text, Text> {

	private static byte[] family = Bytes.toBytes("message");
	private static String[] qualifier;
	static {
		Field[] fields = ClientExceptionReq.class.getDeclaredFields();
		qualifier = new String[fields.length - 1];
		for (int i = 1; i < fields.length; i++) {
			if (StringUtils.isNotBlank(fields[i].getName())) {
				qualifier[i - 1] = fields[i].getName();
			}
		}
	}
	private static String splitStr = ":";
	private static int i=0;
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		i++;
		byte[] r = value.getRow();
		byte[] v = value.getValue(family, "mes_value".getBytes());
		context.write(new Text(Bytes.toString(r)), new Text(Bytes.toString(v)));
	}

}
