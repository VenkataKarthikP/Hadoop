/**
 * 
 */
package com.cloudwick.hadoop.apachelog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Venkata Karthik
 *
 */
public class CustomKey implements WritableComparable<CustomKey>{

	private String ip;
	private String request_page;
	private String timestamp;
	
	
	public CustomKey(){
		
		
	}
	
	public CustomKey(String ip,String request_page,String timestamp){
		if(ip != null && request_page !=null && timestamp != null){
			this.ip = ip;
			this.request_page = request_page;
			this.timestamp= timestamp;
		}
	}
	
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ip = WritableUtils.readString(in);
		request_page = WritableUtils.readString(in);
		timestamp = WritableUtils.readString(in); 
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, ip);
		WritableUtils.writeString(out, request_page);
		WritableUtils.writeString(out, timestamp);
		
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return ip + " "+request_page + " "+timestamp;
	}
	
	public int compareTo(CustomKey o) {
		// TODO Auto-generated method stub
		return this.toString().compareTo(o.toString());
	}

}
