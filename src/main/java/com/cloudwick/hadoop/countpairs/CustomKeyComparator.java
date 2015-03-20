package com.cloudwick.hadoop.countpairs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * @author Venkata Karthik
 *
 */
public class CustomKeyComparator extends WritableComparator {

	protected CustomKeyComparator() {
		super(com.cloudwick.hadoop.countpairs.TextPair.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		TextPair tp1 = (TextPair) a;
		TextPair tp2 = (TextPair) b;
	//	System.out.println("karthik1"+tp1);
		return tp1.toString().compareTo(tp2.toString());
	}
}
