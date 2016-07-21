package com.hadoop.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class MesAllProcessReq implements Writable, DBWritable{

	private Integer id;
	private String code;
	private Integer warnCount;
	private Integer errorCount;
	private String createdTime;
	private String processStatus;
	
	
	@Override
	public void readFields(ResultSet result) throws SQLException {
//		 this.id = result.getInt(1);
         this.code = result.getString(2);
         this.warnCount = result.getInt(3);
         this.errorCount = result.getInt(4);
         this.createdTime =result.getString(5);
         this.processStatus =result.getString(6);
         
	}
	@Override
	public void write(PreparedStatement stmt) throws SQLException {
//		 stmt.setInt(1, this.id);
         stmt.setString(1, this.code);
         stmt.setInt(2, this.warnCount);
         stmt.setInt(3, this.errorCount);
         stmt.setString(4, this.createdTime);
         stmt.setString(5, this.processStatus);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
//		this.id = in.readInt();
		this.code=Text.readString(in);
		this.warnCount=in.readInt();
		this.errorCount=in.readInt();
		this.createdTime=Text.readString(in);
		this.processStatus=Text.readString(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
//		 out.writeInt(this.id);
         Text.writeString(out,this.code);
         out.writeInt(this.warnCount);
         out.writeInt(this.errorCount);
         Text.writeString(out, this.createdTime);
         Text.writeString(out, this.processStatus);
	}
	
	
	@Override
	public String toString() {
		return "MesAllProcessReq [id=" + id + ", code=" + code + ", warnCount=" + warnCount + ", errorCount=" + errorCount + ", createdTime=" + createdTime + ", processStatus=" + processStatus + "]";
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public Integer getWarnCount() {
		return warnCount;
	}
	public void setWarnCount(Integer warnCount) {
		this.warnCount = warnCount;
	}
	public Integer getErrorCount() {
		return errorCount;
	}
	public void setErrorCount(Integer errorCount) {
		this.errorCount = errorCount;
	}
	public String getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(String createdTime) {
		this.createdTime = createdTime;
	}
	public String getProcessStatus() {
		return processStatus;
	}
	public void setProcessStatus(String processStatus) {
		this.processStatus = processStatus;
	}
	
}
