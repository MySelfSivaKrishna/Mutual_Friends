package com.custom_writables;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import com.hirw.facebookfriends.writables.Friend;

public class FriendsArray extends ArrayWritable {

	/**
	 * @param valueClass
	 * @param values
	 */
	public FriendsArray(Class<? extends Writable> valueClass, Writable[] values) {
		super(valueClass, values);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param valueClass
	 */
	public FriendsArray(Class<? extends Writable> valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param strings
	 */
	public FriendsArray() {
		super(Friend.class);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean equals(Object o) {
		Friend[] context_object =Arrays.copyOf(this.get(), this.get().length,Friend[].class);
		Friend[] f2 = Arrays.copyOf(((ArrayWritable) o).get(), ((ArrayWritable) o).get().length, Friend[].class);
		
		boolean result = false;
		
		for(Friend outerf : context_object) {
			result = false;
			for(Friend innerf : f2) {
				if(outerf.equals(innerf)) {
					result=true;
					break;
				}
			}
			if(!result)
				return result;
		}
		 
		return result;
	}

	@Override
	public String toString() {

		Friend[] friendArray = Arrays.copyOf(get(), get().length, Friend[].class);
		String print="";
		
		for(Friend f : friendArray) 
			print+=f;
		
		return print;
	}

}
