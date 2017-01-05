package com.mapreduce.hw2.valueobjects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.Utils;

public class TemperatureValueWritable implements Writable {
	
	private int year;
	private int value;
	private int noOfStations;
	private String type;
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public TemperatureValueWritable(){
		
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public int getNoOfStations() {
		return noOfStations;
	}

	public void setNoOfStations(int noOfStations) {
		this.noOfStations = noOfStations;
	}

	public void write(DataOutput out) throws IOException {
		Utils.writeVInt(out, year);
		Utils.writeVInt(out, value);
		Utils.writeVInt(out, noOfStations); 	
		Utils.writeString(out, type);
	}

	public void readFields(DataInput in) throws IOException {
		year = Utils.readVInt(in);
		value = Utils.readVInt(in);
		noOfStations = Utils.readVInt(in);
		type = Utils.readString(in);
	}

}
