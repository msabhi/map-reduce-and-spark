package com.mapreduce.hw2.valueobjects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.Utils;

/*
 * Custom writable object used as intermediate value object
 * containing the TMAX/TMIN values
 */
public class TempValueWritable implements Writable {
	
	private int year;
	private int TMAX;
	private int noOfTmaxStations;
	private int TMIN;
	private int noOfTminStations;
	
	public TempValueWritable(int year, int TMAX, int noOfTmaxStations, int TMIN, int noOfTminStations){
		this.year = year;
		this.TMAX = TMAX;
		this.noOfTmaxStations = noOfTmaxStations;
		this.TMIN = TMIN;
		this.noOfTminStations = noOfTminStations;
	}
	
	public int getTMAX() {
		return TMAX;
	}

	public void setTMAX(int tMAX) {
		TMAX = tMAX;
	}

	public int getNoOfTmaxStations() {
		return noOfTmaxStations;
	}

	public void setNoOfTmaxStations(int noOfTmaxStations) {
		this.noOfTmaxStations = noOfTmaxStations;
	}

	public int getTMIN() {
		return TMIN;
	}

	public void setTMIN(int tMIN) {
		TMIN = tMIN;
	}

	public int getNoOfTminStations() {
		return noOfTminStations;
	}

	public void setNoOfTminStations(int noOfTminStations) {
		this.noOfTminStations = noOfTminStations;
	}


	public TempValueWritable(){
		
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}


	public void write(DataOutput out) throws IOException {
		Utils.writeVInt(out, year);
		Utils.writeVInt(out, TMAX);
		Utils.writeVInt(out, noOfTmaxStations);
		Utils.writeVInt(out, TMIN);
		Utils.writeVInt(out, noOfTminStations);
	}

	public void readFields(DataInput in) throws IOException {
		year = Utils.readVInt(in);
		TMAX = Utils.readVInt(in);
		noOfTmaxStations = Utils.readVInt(in);
		TMIN = Utils.readVInt(in);
		noOfTminStations = Utils.readVInt(in);
	}

	public String toString(){
		return this.year + " | " + this.noOfTmaxStations + " | " + this.getTMAX() + " | " + this.getNoOfTminStations();
	}
}
