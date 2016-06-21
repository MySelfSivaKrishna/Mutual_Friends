package com.custom_writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FriendPair implements WritableComparable{
	
	private Friend f1;
	private Friend f2;

	/**
	 * 
	 */
	public FriendPair() {
		this.f1 = new Friend();
		this.f2 = new Friend();
	}

	public Friend getF1() {
		return f1;
	}

	public void setF1(Friend f1) {
		this.f1 = f1;
	}

	public Friend getF2() {
		return f2;
	}

	public void setF2(Friend f2) {
		this.f2 = f2;
	}

	@Override
	public boolean equals(Object obj) {
		
		if(getF1().compareTo(((FriendPair)obj).f1)==0&&getF2().compareTo(((FriendPair)obj).f2)==0){
			return true;
		}
		if(getF2().compareTo(((FriendPair)obj).f1)==0&&getF1().compareTo(((FriendPair)obj).f2)==0){
			return true;
		}
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return f1.getId().hashCode() + f2.getId().hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "[ "+f1+" "+f2+" ]";
	}

	/**
	 * @param f1
	 * @param f2
	 */
	public FriendPair(Friend f1, Friend f2) {
		super();
		this.f1 = f1;
		this.f2 = f2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		f1.write(out);
		f2.write(out);
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		f1.readFields(in);
		f2.readFields(in);
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(Object o) {
		FriendPair fp2 = (FriendPair) o;
		int cmp = -1;
		if(getF1().compareTo(fp2.getF1())==0||getF1().compareTo(fp2.getF2())==0)
			return cmp=0;
		if(getF2().compareTo(fp2.getF1())==0||getF2().compareTo(fp2.getF2())==0)
			return cmp=0;
		// TODO Auto-generated method stub
		return cmp;
	}

	
}
