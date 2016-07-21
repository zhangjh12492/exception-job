package com.hadoop.common;



public enum ErrLevelEnum {

	WARNING("1"),	//1.代表警告
	ERROR("2");		//2.代表报错
	
	private String code;
	
	private ErrLevelEnum(String code){
		this.code=code;
	}
	public String getCode(){
		return code;
	}
}
