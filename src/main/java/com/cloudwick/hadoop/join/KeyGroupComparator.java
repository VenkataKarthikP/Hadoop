package com.cloudwick.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupComparator  extends WritableComparator{

	protected KeyGroupComparator() {
		super(CustomInput.class,true);
		// TODO Auto-generated constructor stub
	}

	
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		CustomInput key1 = (CustomInput) w1;
		CustomInput key2 = (CustomInput) w2;
		
		
		return key1.did.compareTo(key2.did);
		
		
	}
	
}
