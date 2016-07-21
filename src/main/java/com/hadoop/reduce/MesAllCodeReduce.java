package com.hadoop.reduce;

import com.hadoop.common.ErrLevelEnum;
import com.hadoop.common.SysConstants;
import com.hadoop.util.DateUtils;
import com.hadoop.util.StringUtils;
import com.hadoop.vo.BusiInfo;
import com.hadoop.vo.ClientExceptionReq;
import com.hadoop.vo.MesAllProcessReq;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MesAllCodeReduce extends Reducer<Text, Text, IntWritable, Text> {

	private static Logger log = LoggerFactory.getLogger(MesAllCodeReduce.class);
	/*系统数量统计map*/
	private Map<String, Integer> sysWarnMap = new HashMap<String, Integer>();
	private Map<String, Integer> sysErrorMap = new HashMap<String, Integer>();
	/*业务数量统计map*/
	private Map<String, Integer> busiWarnMap = new HashMap<String, Integer>();
	private Map<String, Integer> busiErrorMap = new HashMap<String, Integer>();
	/*异常数量统计map*/
	private Map<String, Integer> errWarnMap = new HashMap<String, Integer>();
	private Map<String, Integer> errErrorMap = new HashMap<String, Integer>();

	private Map<String, BusiInfo> sysBusiMap = new HashMap<String, BusiInfo>();
	private ObjectMapper objectMapper = new ObjectMapper();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("errId:" + key.toString());
		String errId = null; //异常信息编号
		String keys[] = null; //异常信息编号拆分成数组
		ClientExceptionReq clientReq = null;

		for (Text t : values) {
			//			clientReq=JSONObject.parseObject(t.toString(),ClientExceptionReq.class);
			clientReq = objectMapper.readValue(t.toString(), ClientExceptionReq.class);
			errId = clientReq.getErrId();
		}
		System.out.println("errId:" + errId);
		System.out.println("value:" + clientReq.toString());
		if (StringUtils.isBlank(errId)) {
			return;
		}
		BusiInfo busiInfo = new BusiInfo();
		busiInfo.setSysCode(clientReq.getSysCode());
		busiInfo.setBusiCode(clientReq.getBusiCode());
		busiInfo.setBusiDesc(clientReq.getBusiDesc());

		sysBusiMap.put(clientReq.getSysCode() + clientReq.getBusiCode(), busiInfo);

		keys = qualifierConvert(errId);
		String errLevel = keys[0];
		String sysCode = keys[2];
		String busiCode = keys[2] + keys[3];
		String errCode = keys[2] + keys[3] + keys[4];
		/*初始化所有的map*/
		if (sysWarnMap.get(sysCode) == null) {
			sysWarnMap.put(sysCode, 0);
		}
		if (sysErrorMap.get(sysCode) == null) {
			sysErrorMap.put(sysCode, 0);
		}
		if (busiWarnMap.get(busiCode) == null) { 
			busiWarnMap.put(busiCode, 0);
		}
		if (busiErrorMap.get(busiCode) == null) {
			busiErrorMap.put(busiCode, 0);
		}
		if (errErrorMap.get(errCode) == null) {
			errErrorMap.put(errCode, 0);
		}
		if (errWarnMap.get(errCode) == null) {
			errWarnMap.put(errCode, 0);
		}
		/**/
		if (StringUtils.isNotBlank(errLevel)) {
			//warn
			if (errLevel.equals(ErrLevelEnum.WARNING.getCode())) {
				sysWarnMap.put(sysCode, sysWarnMap.get(sysCode) + 1);
				busiWarnMap.put(busiCode, busiWarnMap.get(busiCode) + 1);
				errWarnMap.put(errCode, errWarnMap.get(errCode) + 1);

			}
			//error
			if (errLevel.equals(ErrLevelEnum.ERROR.getCode())) {
				sysErrorMap.put(sysCode, sysErrorMap.get(sysCode) + 1);
				busiErrorMap.put(busiCode, busiErrorMap.get(busiCode) + 1);
				errErrorMap.put(errCode, errErrorMap.get(errCode) + 1);
			}
		}
//		context.write(new Text(sysCode), new Text(objectMapper.writeValueAsString(errErrorMap)));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		List<MesAllProcessReq> mesAllProceList = new ArrayList<MesAllProcessReq>();
		for(Entry<String, Integer> sysEntry:sysWarnMap.entrySet()){
			MesAllProcessReq mesAllproce=new MesAllProcessReq();
			mesAllproce.setCode(sysEntry.getKey());
			mesAllproce.setWarnCount(sysWarnMap.get(sysEntry.getKey()));
			mesAllproce.setErrorCount(sysErrorMap.get(sysEntry.getKey()));
			mesAllproce.setCreatedTime(DateUtils.getCurrentDate());
			mesAllProceList.add(mesAllproce);
		}
		for(Entry<String, Integer> sysEntry:busiWarnMap.entrySet()){
			MesAllProcessReq mesAllproce=new MesAllProcessReq();
			mesAllproce.setCode(sysEntry.getKey());
			mesAllproce.setWarnCount(busiWarnMap.get(sysEntry.getKey()));
			mesAllproce.setErrorCount(busiErrorMap.get(sysEntry.getKey()));
			mesAllproce.setCreatedTime(DateUtils.getCurrentDate());
			mesAllProceList.add(mesAllproce);
		}
		for(Entry<String, Integer> sysEntry:errWarnMap.entrySet()){
			MesAllProcessReq mesAllproce=new MesAllProcessReq();
			mesAllproce.setCode(sysEntry.getKey());
			mesAllproce.setWarnCount(errWarnMap.get(sysEntry.getKey()));
			mesAllproce.setErrorCount(errErrorMap.get(sysEntry.getKey()));
			mesAllproce.setCreatedTime(DateUtils.getCurrentDate());
			mesAllProceList.add(mesAllproce);
		}

		for(int i=0;i<mesAllProceList.size();i++){
			MesAllProcessReq mes=mesAllProceList.get(i);
			context.write(new IntWritable(), new Text(SysConstants.REDUCE_MESALLPROCESS_KEY+"\t"+objectMapper.writeValueAsString(mes)));
		}
		for (BusiInfo busiInfo:sysBusiMap.values()){
			context.write(new IntWritable(), new Text(SysConstants.REDUCE_BUSIINFO_KEY+"\t"+objectMapper.writeValueAsString(busiInfo)));
		}


		super.cleanup(context);
	}

	/**
	 * 插入业务数据,待用
	 */
//	public static void insertBusiByReduce(Map<String, BusiInfo> busis) {
//		try {
//			BusiInfo busiInfo = new BusiInfo();
//			busiInfo.setBusiCode("02");
//			ObjectMapper objectMapper = new ObjectMapper();
//			String params = objectMapper.writeValueAsString(busis);
//			System.out.println("开始插入业务数据:" + params);
//			//			HttpClientUtil.postJSON("http://172.16.3.202:8080/exceptionSocketPro/busi.do?insertBusiByReduce", params);
//			log.info("发送业务数据成功!");
//		} catch (Exception e) {
//			System.out.println("发送业务数据失败");
//			e.printStackTrace();
//		}
//	}

	/**
	 * 转换errId
	 * @param errId
	 * 生成规则,errLevel+flag+sysCode+busiCode+errCode+sysErrCode+timeStamp	
	 * @return
	 */
	private static String[] qualifierConvert(String errId) {
		String[] str = new String[7];
		str[0] = errId.substring(0, 1); //errLevel
		str[1] = errId.substring(1, 2); //flag
		str[2] = errId.substring(2, 4); //sysCode
		str[3] = errId.substring(4, 7); //busiCode
		str[4] = errId.substring(7, 10); //errCode
		str[5] = errId.substring(10, 13); //sysErrCode
		str[6] = errId.substring(13); //timeStamp
		return str;
	}

}
