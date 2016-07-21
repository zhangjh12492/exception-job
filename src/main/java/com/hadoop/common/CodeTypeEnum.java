package com.hadoop.common;


public enum CodeTypeEnum {

	SYS("sys"),	//系统
	BUSI("busi"),		//业务类型
	ERR("err"),		//用户自定义异常类型
	SYSERR("sysErr");		//系统产生的异常类型
	
	private String code;
	
	private CodeTypeEnum(String code){
		this.code=code;
	}
	public String getCode(){
		return code;
	}
}
