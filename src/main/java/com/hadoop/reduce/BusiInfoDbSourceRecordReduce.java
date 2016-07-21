package com.hadoop.reduce;

import com.hadoop.common.SysConstants;
import com.hadoop.job.MesAllCountUnprocessJob;
import com.hadoop.vo.BusiInfo;
import com.hadoop.vo.MesAllProcessReq;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2015/10/12.
 */
public class BusiInfoDbSourceRecordReduce extends MapReduceBase implements Reducer<LongWritable, Text, BusiInfo, Text> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void reduce(LongWritable key, Iterator<Text> val, OutputCollector<BusiInfo, Text> output, Reporter reporter) throws IOException {

        BusiInfo busiInfo = null;
        while (val.hasNext()) {

            String value = val.next().toString();
            String str[]=value.split("\t");
            if(str[1].trim().equals(SysConstants.REDUCE_BUSIINFO_KEY)){
                busiInfo = objectMapper.readValue(str[2], BusiInfo.class);
            }
        }
        if (busiInfo != null) {

            output.collect(busiInfo, new Text());
        }
    }
}
