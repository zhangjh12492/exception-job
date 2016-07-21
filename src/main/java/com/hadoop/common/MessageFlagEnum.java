package com.hadoop.common;

public enum MessageFlagEnum {

	VALID("0"),		//is valid
	INVALID("1");	// not valid
	
	private MessageFlagEnum(String code){
		this.code=code;
	}
	
	private String code;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
	
}
