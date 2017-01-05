package com.mapreduce.runnables;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mapreduce.dataobject.StationInfo;
import com.mapreduce.dataobject.SynchronizedStationInfo;
import com.mapreduce.hw1.q1.EntryPoint;

public class FineLockFibRunnable implements Runnable {

	// Shared data strucuture to maintain the word count
	public static Map<String, StationInfo> stationTotalTMAXMap = new ConcurrentHashMap<>();

	private int listStart = 0;
	private int listEnd = 0;
	private List<String> fileList = null;

	// This class has to initialized with a task
	private FineLockFibRunnable() {}

	// initialize with a task
	public FineLockFibRunnable(int start, int end, List<String> fileList){
		listStart = start;
		listEnd = end;
		this.fileList = fileList;
	}

	@Override
	public void run() {

		for(int i=listStart; i<listEnd; i++){

			String[] cols = fileList.get(i).split(",");

			if(cols[2].trim().equals("TMAX")){
				// Fetch the station id object from the concurrent map
				SynchronizedStationInfo prevStationInfo = stationTotalTMAXMap.get(cols[0]);

				// if this is the first station id key to be inserted 
				if (prevStationInfo == null){

					//update the map using putIfAbsent which only puts if the key is really absent and returns null
					//if key is not present, returns the existing StationInfo object for the key and doesn't update the map
					prevStationInfo = stationTotalTMAXMap.putIfAbsent(cols[0], new StationInfo(Integer.parseInt(cols[3])));

					//if the key existed already due to another thread, perform a atomic update on the object returned by the map
					if(prevStationInfo != null){
						prevStationInfo.atomicUpdateStationInfoWithFib(Integer.parseInt(cols[3]));
					}
				}
				else{
					// if we are here, the key already existed in the map and we have to perform a atomic
					// update on the object
					prevStationInfo.atomicUpdateStationInfoWithFib(Integer.parseInt(cols[3]));
				}


				// Java 8 has introduced a new way of doing a get and put as atomic operation - compute
				// compute takes in a call back function which executes upon locking 
				// However, I couldn't confirm on whether its locking the whole map or only the key-value pair

				// stationTotalTMAXMap.compute(cols[0], (key, value)-> value == null?
				//		new StationInfo().updateStationInfo(Integer.parseInt(cols[3])) : value.updateStationInfo(Integer.parseInt(cols[3])));
			}
		}
	}


	public static void fineLockExecutor(List<String> fileList) throws InterruptedException{
		int noOfCores = Runtime.getRuntime().availableProcessors();
		int jobsPerThread = fileList.size()/noOfCores;
		double avg = 0;
		long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for(int j=0; j<10; j++){
			int start = 0;
			stationTotalTMAXMap = new ConcurrentHashMap<>();
			ExecutorService executorService = Executors.newFixedThreadPool(noOfCores);
			long startTime = System.currentTimeMillis();
			for(int i=0; i<noOfCores; i++){
				executorService.execute(new FineLockFibRunnable(start, start+jobsPerThread, fileList));
				start += jobsPerThread;
			}

			executorService.shutdown();		
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long endTime = System.currentTimeMillis();
			max = Math.max(max,  endTime-startTime);
			min = Math.min(min, endTime-startTime);
			avg += endTime-startTime;
		}
		EntryPoint.printMessage(max, min, avg/10, "Fine lock execution with Fibonacci");
	}

}
