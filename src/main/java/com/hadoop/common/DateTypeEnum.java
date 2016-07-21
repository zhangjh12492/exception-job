package com.hadoop.common;


public enum DateTypeEnum {
	
	TODAY("today"),	//今天
	YESTERDAY("yesterday"),	//昨天
	MOREEARLY("moreEarly"),	//更早的
	HOUR("hour"),	//小时
	DAY("day"),		//一天
	WEEK("week"),		//一周
	MONTH("month"),		//一个月
	QUARTER("quarter"),		//一季度
	HALFYEAR("halfYear"),		//半年
	YEAR("year"),		//一年
	ALL("all");	//所有时间的 
	
	private String code;
	
	private DateTypeEnum(String code){
		this.code=code;
	}
	public String getCode(){
		return code;
	}
}
