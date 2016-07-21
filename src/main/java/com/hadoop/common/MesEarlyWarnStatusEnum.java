package com.hadoop.common;


public enum MesEarlyWarnStatusEnum {

	START("0"),
	STOP("1");
	
	private String code;
	private MesEarlyWarnStatusEnum(String code){
		this.code=code;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	
}
