package com.mapreduce.dataobject;

public interface SynchronizedStationInfo {
	
	public void atomicUpdateStationInfo(long tmaxVal);
	public void atomicUpdateStationInfoWithFib(long tmaxVal);

}
