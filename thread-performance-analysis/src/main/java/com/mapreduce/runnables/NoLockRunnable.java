package com.mapreduce.runnables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mapreduce.dataobject.StationInfo;
import com.mapreduce.hw1.q1.EntryPoint;

public class NoLockRunnable implements Runnable{

	// Shared data strucuture to maintain the word count
	public static Map<String, StationInfo> stationTotalTMAXMap = new HashMap<>();

	private int listStart = 0;
	private int listEnd = 0;
	private List<String> fileList = null;

	private NoLockRunnable() {}

	public NoLockRunnable(int start, int end, List<String> fileList){
		listStart = start;
		listEnd = end;
		this.fileList = fileList;
	}

	@Override
	public void run() {
		for(int i=listStart; i<listEnd; i++){
			String[] cols = fileList.get(i).split(",");
			if(cols[2].equals("TMAX")){
				//No lock on the map which updating 
				//Race condition 
				stationTotalTMAXMap.put(cols[0], stationTotalTMAXMap.getOrDefault(cols[0], 
						new StationInfo()).updateStationInfo(Integer.parseInt(cols[3])));
			}
		}
	}

	public static void noLockExecutor(List<String> fileList) throws InterruptedException{
		int noOfCores = Runtime.getRuntime().availableProcessors();
		int jobsPerThread = fileList.size()/noOfCores;
		double avg = 0;
		long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for(int i=0; i<10; i++){
			int start = 0;
			stationTotalTMAXMap = new HashMap<>();
			ExecutorService executorService = Executors.newFixedThreadPool(noOfCores);
			long startTime = System.currentTimeMillis();
			for(int j=0; j<noOfCores; j++){
				executorService.execute(new NoLockRunnable(start, start+jobsPerThread, fileList));
				start += jobsPerThread;
			}
			executorService.shutdown();		
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long endTime = System.currentTimeMillis();
			max = Math.max(max,  endTime-startTime);
			min = Math.min(min, endTime-startTime);
			avg += endTime-startTime;	
		}
		EntryPoint.printMessage(max, min, avg/10, "No lock execution without Fibonacci");
	}

}
