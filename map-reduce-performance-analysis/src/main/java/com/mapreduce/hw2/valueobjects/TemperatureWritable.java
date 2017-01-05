package com.mapreduce.hw2.valueobjects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TemperatureWritable implements Writable{
	
	private Text type; 
	private IntWritable noOfStations;
	private IntWritable value;
	
	public TemperatureWritable(){
		type = new Text();
		noOfStations = new IntWritable(0);
		value = new IntWritable(0);
	}
	
	public TemperatureWritable(Text type, IntWritable value){	
		this.type = type;
		this.value = value;
		this.noOfStations = new IntWritable(1);
	}
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		noOfStations.readFields(in);
		value.readFields(in);		
	}

	public void write(DataOutput out) throws IOException {
		type.write(out);
		noOfStations.write(out);
		value.write(out);
	}
	
	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}

	public IntWritable getNoOfStations() {
		return noOfStations;
	}

	public void setNoOfStations(IntWritable noOfStations) {
		this.noOfStations = noOfStations;
	}

	public IntWritable getValue() {
		return value;
	}

	public void setValue(IntWritable value) {
		this.value = value;
	}
	
	public void update(int value, int noOfStations){
		IntWritable newValue = new IntWritable(this.value.get() + value);
		IntWritable newNoOfStations = new IntWritable(this.noOfStations.get() + noOfStations);
		setValue(newValue);
		setNoOfStations(newNoOfStations);
	}
	
}
