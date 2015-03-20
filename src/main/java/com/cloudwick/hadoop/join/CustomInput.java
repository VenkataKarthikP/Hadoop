package com.cloudwick.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CustomInput  implements WritableComparable{
	
	public String did;
	public int id;
	public CustomInput()
	{
		
		
	}
	public CustomInput(String did, int id )
	{
		this.did = did;
		this.id = id;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		did = in.readLine();
		id = in.readInt();
	
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBytes(did);
		out.writeInt(id);
		
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		
		CustomInput ct = (CustomInput)o;
		
		if(ct.id == 0)
			return 1;
		else
			return this.did.compareTo(ct.did);
		
	}

}
