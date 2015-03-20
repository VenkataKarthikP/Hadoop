package com.cloudwick.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Venkata Karthik
 *
 */
public class CompositeKeyComparator extends WritableComparator{

	protected CompositeKeyComparator() {
		super(CustomInput.class,true);
	}

/* (non-Javadoc)
 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
 */
public int compare(WritableComparable w1, WritableComparable w2) {
		
		CustomInput key1 = (CustomInput) w1;
		CustomInput key2 = (CustomInput) w2;

		if(key1.id == 0)
			return 1;
		if(key2.id == 0)
			return -1;
		
		return key1.did.compareTo(key2.did);
}

}

