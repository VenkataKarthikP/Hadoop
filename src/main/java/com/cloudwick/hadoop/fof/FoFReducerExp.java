package com.cloudwick.hadoop.fof;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FoFReducerExp  extends Reducer<Text, Text, Text, Text>{
	
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws java.io.IOException ,InterruptedException 
	{
		
		Iterable<Text> arg3=arg1;
	    Iterator<Text> itr = arg1.iterator();
	    String temp = null;
	    HashSet<String>  alreadyfriends = new HashSet();
	    String output = "";
	    HashSet<String> output1 = new HashSet();
		System.out.println(arg0.toString());
		
		java.util.List<String> list = new ArrayList<String>();
		
			while(itr.hasNext())
		    {
				//System.out.println(itr.next());
				String temp1  = new String(itr.next().toString());
				System.out.println("Iterator elements are " + temp1);
				list.add(temp1);
				
		    }
			
			System.out.println("List is" + list);
			Iterator<String> itr1 = list.iterator();
			
	    while(itr1.hasNext())
	    {
	    	System.out.println("Here 1");
	    	
	    	temp = new String(itr1.next());
	    	System.out.println(temp);
	    	String[] temp_arr = temp.split(" ");
	    	
	    	//temp_arr[0] temp_arr[1]
	    	
	    	if(temp_arr[1].equals("f"))
	    	{
	    		alreadyfriends.add(temp_arr[0]);
	    	}
	
	    }
		
	    System.out.println("Set is " + alreadyfriends);
	    Iterator<String> itr3 = list.iterator();
	    
	    while(itr3.hasNext())
	    {
	    	System.out.println("IM in next loop");
	    	temp = itr3.next();
	    	String[] temp_arr = temp.split(" ");
	    	
	    	if(temp_arr[1].equals("s"))
	    	{
	    		if( ! alreadyfriends.contains(temp_arr[0]))
	    		{
	    			System.out.println("Updating Output");
	    			output = output + temp_arr[0]+" ";
	    			
	    			output1.add(temp_arr[0]);
	    			
	    		}
	    		
	    	}
	   
	    }
	    
	    
	    
		arg2.write(arg0, new Text(output1.toString()));
		
	}

}
