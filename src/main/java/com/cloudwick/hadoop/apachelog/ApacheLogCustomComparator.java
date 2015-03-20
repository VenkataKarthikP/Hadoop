package com.cloudwick.hadoop.apachelog;

import org.apache.hadoop.io.WritableComparator;



public class ApacheLogCustomComparator extends WritableComparator {

	protected ApacheLogCustomComparator(){
		
		super(CustomKey.class, true);
	}
	
	public int compare(CustomKey o1, CustomKey o2) {
		// TODO Auto-generated method stub
		return o1.toString().compareTo(o2.toString());
	}

}
