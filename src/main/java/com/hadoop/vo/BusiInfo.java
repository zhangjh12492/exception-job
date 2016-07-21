package com.hadoop.vo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BusiInfo  implements Writable, DBWritable {

	private Integer id;
	private String busiCode;
	private String busiDesc;
	private Integer sysId;
	private String sysDesc;
	private String sysCode;

	public BusiInfo(){}
	
    public BusiInfo(Integer id, String busiCode, String busiDesc, Integer sysId, String sysDesc, String sysCode) {
		super();
		this.id = id;
		this.busiCode = busiCode;
		this.busiDesc = busiDesc;
		this.sysId = sysId;
		this.sysDesc = sysDesc;
		this.sysCode = sysCode;
	}


    @Override
    public void readFields(ResultSet result) throws SQLException {
////		 this.id = result.getInt(1);
//        this.busiCode = result.getString(2);
//        this.busiDesc = result.getString(3);
//        this.errorCount = result.getInt(4);
//        this.createdTime =result.getString(5);
//        this.processStatus =result.getString(6);

    }
    @Override
    public void write(PreparedStatement stmt) throws SQLException {
//		 stmt.setInt(1, this.id);
        stmt.setString(1, this.busiCode);
        stmt.setString(2, this.busiDesc);
        stmt.setString(3, this.sysCode);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
////		this.id = in.readInt();
        this.busiCode= Text.readString(in);
        this.busiDesc= Text.readString(in);
        this.sysCode= Text.readString(in);

    }
    @Override
    public void write(DataOutput out) throws IOException {
//		 out.writeInt(this.id);
        Text.writeString(out, this.busiCode);
        Text.writeString(out, this.busiDesc);
        Text.writeString(out, this.sysCode);
    }


	public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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

    public Integer getSysId() {
        return sysId;
    }

    public void setSysId(Integer sysId) {
        this.sysId = sysId;
    }

    public String getSysDesc() {
        return sysDesc;
    }

    public void setSysDesc(String sysDesc) {
        this.sysDesc = sysDesc;
    }

    public String getSysCode() {
        return sysCode;
    }

    public void setSysCode(String sysCode) {
        this.sysCode = sysCode;
    }
}
