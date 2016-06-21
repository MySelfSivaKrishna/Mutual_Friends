package com.custom_writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Friend implements WritableComparable{
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.Id.hashCode();
	}

	private IntWritable Id;
	private Text Name;
	private Text HomeTown;

	/**
	 * 
	 */
	public Friend() {
	this.Id = new IntWritable();
	this.Name = new Text();
	this.HomeTown = new Text();
	
	}

	public Friend(IntWritable id, Text name, Text homeTown) {
		super();
		Id = id;
		Name = name;
		HomeTown = homeTown;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Id.readFields(in);
		Name.readFields(in);
		HomeTown.readFields(in);
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Id.write(out);
		Name.write(out);
		HomeTown.write(out);
	}


	public IntWritable getId() {
		return Id;
	}

	public void setId(IntWritable id) {
		Id = id;
	}

	public Text getName() {
		return Name;
	}

	public void setName(Text name) {
		Name = name;
	}

	public Text getHomeTown() {
		return HomeTown;
	}

	public void setHomeTown(Text homeTown) {
		HomeTown = homeTown;
	}

	@Override
	public int compareTo(Object o) {
		

		return getId().compareTo(((Friend) o).getId());
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return getId().equals(obj);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.Id + ":" + this.Name ;
	}

}
