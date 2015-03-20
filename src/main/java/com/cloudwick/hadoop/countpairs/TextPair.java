package com.cloudwick.hadoop.countpairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


/**
 * @author Venkata Karthik
 *
 */
public class TextPair implements WritableComparable<TextPair>{


	private String arg0;
	private String arg1;

	/**
	 * 
	 */
	public TextPair()
	{
		
	}
	/**
	 * @param arg0
	 * @param arg1
	 */
	public TextPair(String arg0, String arg1) {
		// TODO Auto-generated constructor stub
		this.arg0 = arg0;
		this.arg1 = arg1;
		
	
	}
	  
	 
	@Override
	public String toString() {
		// TODO Auto-generated method stub
	
		return this.arg0 +" "+ this.arg1;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		 
		arg0 = WritableUtils.readString(in);
		arg1 = WritableUtils.readString(in);
	}
		 
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
		 */
		public void write(DataOutput out) throws IOException {
		 
		WritableUtils.writeString(out, arg0);
		WritableUtils.writeString(out, arg1);
		}
		 
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(TextPair o) {
		
		return this.toString().compareTo(((TextPair)o).toString());
	}
	
	
}
