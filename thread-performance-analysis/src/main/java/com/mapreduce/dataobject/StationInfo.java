package com.mapreduce.dataobject;

import com.mapreduce.hw1.q1.EntryPoint;

public class StationInfo implements SynchronizedStationInfo{

	private long TMAX = 0;
	private long noOfStations = 0;
	
	public StationInfo(){
	}
	
	public StationInfo(long tmax){
		TMAX += tmax;
		noOfStations ++;
	}
	
	public StationInfo(long tmax, long noOfStations){
		TMAX = tmax;
		this.noOfStations = noOfStations;
	}
	
	public long getTMAX(){
		return this.TMAX;
	}
	
	public void addStations(long noOfStations){
		this.noOfStations += noOfStations;
	}
	public long getNoOfStations(){
		return this.noOfStations;
	}
	
	public long getAverage(){
		return TMAX/noOfStations;
	}
	
	public void addTMAX(long tmaxVal){
		this.TMAX += tmaxVal;
	}
	
	public StationInfo updateStationInfo(long tmaxVal){
		this.TMAX += tmaxVal;
		noOfStations++;
		return this;
	}
	//Update the object along with fibonacci 
	public StationInfo updateStationInfoWithFib(long tmaxVal){
		EntryPoint.doFibonacci(17);
		this.TMAX += tmaxVal;
		noOfStations++;
		return this;
	}
	
	
	public String toString(){
		return TMAX + " | " + noOfStations;
	}

	@Override
	// Used only for Fine lock where we need to only lock the accumulation
	// data structure
	public synchronized void atomicUpdateStationInfo(long tmaxVal) {
		this.TMAX += tmaxVal;
		noOfStations++;
	}
	
	// Used only for Fine lock where we need to only lock the accumulation
	// data structure
	public synchronized void atomicUpdateStationInfoWithFib(long tmaxVal) {
		this.TMAX += tmaxVal;
		noOfStations++;
		EntryPoint.doFibonacci(17);
	}
}
