package com.hadoop.common;

import org.apache.hadoop.hbase.util.Bytes;

public class TableParamCommon {
	public static byte[] tableName=Bytes.toBytes("mesReq");
	public static byte[] family=Bytes.toBytes("message");
	public static String [] qualifier=new String[]{ "sysCode", "busiCode", "busiDesc", "errCode", "errDesc", "sysErrCode", "sysErrDesc", "throwableDesc", "createDate", "errLevel","flag" };
	public static String splitStr=":";
	public static String []sysCodes=new String[]{"01","02","03"};
	
}
