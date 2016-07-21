package com.hadoop.common;

public enum ProcessStatusEnum {

	UNDISPOSED("0"),		//the status is undisposed. 未处理
	PROCESSING("1"),	//the status is processed. 正在处理
	PROCESSEND("2");	//2.process end	已处理
	
	private String code;
	
	private ProcessStatusEnum(String code){
		this.code=code;
	}
	public String getCode(){
		return code;
	}
}
