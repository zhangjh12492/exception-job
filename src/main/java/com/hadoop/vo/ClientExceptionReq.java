package com.hadoop.vo;

import java.io.Serializable;

public class ClientExceptionReq implements Serializable{

	/** 属性名称 */
	private static final long serialVersionUID = 1L;

	private String errId;	//异常序列编号,生成规则,errLevel+flag+sysCode+busiCode+errCode+sysErrCode+timeStamp	 
	private String sysCode;	//系统编码 
	private String busiCode;	//业务编码
	private String busiDesc;	//业务描述
	private String errCode;		//系统自定义异常编码
	private String errDesc;		//系统自定义异常描述
	private String sysErrCode;	//系统异常编码
	private String sysErrDesc;	//系统异常描述
	private String throwableDesc;	//异常详细信息
	private String createDate;	//创建日期
	private String errLevel;	//异常等级
	private String flag;		//is valid   0:true  1:false
	private String processStatus;	//process status, 0.no process,1.processing,2.process end
//	String [] qualifiers=new String[]{"errId","sysCode","busiCode","busiDesc","errCode","errDesc","sysErrCode","sysErrDesc","throwableDesc"};
	public String getErrId() {
		return errId;
	}
	public void setErrId(String errId) {
		this.errId = errId;
	}
	public String getSysCode() {
		return sysCode;
	}
	public void setSysCode(String sysCode) {
		this.sysCode = sysCode;
	}
	public String getBusiCode() {
		return busiCode;
	}
	public void setBusiCode(String busiCode) {
		this.busiCode = busiCode;
	}
	public String getBusiDesc() {
		return busiDesc;
	}
	public void setBusiDesc(String busiDesc) {
		this.busiDesc = busiDesc;
	}
	public String getErrCode() {
		return errCode;
	}
	public void setErrCode(String errCode) {
		this.errCode = errCode;
	}
	public String getErrDesc() {
		return errDesc;
	}
	public void setErrDesc(String errDesc) {
		this.errDesc = errDesc;
	}
	public String getThrowableDesc() {
		return throwableDesc;
	}
	public void setThrowableDesc(String throwableDesc) {
		this.throwableDesc = throwableDesc;
	}
	public String getCreateDate() {
		return createDate;
	}
	public void setCreateDate(String createDate) {
		this.createDate = createDate;
	}
	public String getSysErrCode() {
		return sysErrCode;
	}
	public void setSysErrCode(String sysErrCode) {
		this.sysErrCode = sysErrCode;
	}
	public String getSysErrDesc() {
		return sysErrDesc;
	}
	public void setSysErrDesc(String sysErrDesc) {
		this.sysErrDesc = sysErrDesc;
	}
	
	public String getErrLevel() {
		return errLevel;
	}
	public void setErrLevel(String errLevel) {
		this.errLevel = errLevel;
	}
	public String getFlag() {
		return flag;
	}
	public void setFlag(String flag) {
		this.flag = flag;
	}
	
	public String getProcessStatus() {
		return processStatus;
	}
	public void setProcessStatus(String processStatus) {
		this.processStatus = processStatus;
	}
	@Override
	public String toString() {
		return "ClientExceptionReq [errId=" + errId + ", sysCode=" + sysCode + ", busiCode=" + busiCode + ", busiDesc=" + busiDesc + ", errCode=" + errCode + ", errDesc=" + errDesc + ", sysErrCode="
				+ sysErrCode + ", sysErrDesc=" + sysErrDesc + ", throwableDesc=" + throwableDesc + ", createDate=" + createDate + ", errLevel=" + errLevel + ", flag=" + flag + ", processStatus="
				+ processStatus + "]";
	}

	
	
	
	
	
}
