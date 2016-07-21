package com.hadoop.reduce;

import java.io.IOException;
import java.util.Iterator;

import com.hadoop.common.SysConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.map.ObjectMapper;

import com.hadoop.job.MesAllCountUnprocessJob;
import com.hadoop.vo.MesAllProcessReq;

/**
 *
 * @ClassName: DbSourceRecord
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author ZJHao
 * @date 2015-10-1 下午3:02:54
 *
 */
public class MesAllProcessDbSourceRecordReduce extends MapReduceBase implements Reducer<LongWritable, Text, MesAllProcessReq, Text> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void reduce(LongWritable key, Iterator<Text> val, OutputCollector<MesAllProcessReq, Text> output, Reporter reporter) throws IOException {
        MesAllProcessReq mesAllProcessReq = null;
        while (val.hasNext()) {

            String value = val.next().toString();
            String str[]=value.split("\t");
            if(str[1].trim().equals(SysConstants.REDUCE_MESALLPROCESS_KEY)){
                mesAllProcessReq = objectMapper.readValue(str[2], MesAllProcessReq.class);
            }
        }
        if (mesAllProcessReq != null) {

            MesAllCountUnprocessJob.mesAllProcessReq=mesAllProcessReq;
            output.collect(mesAllProcessReq, new Text());
        }
    }


}
