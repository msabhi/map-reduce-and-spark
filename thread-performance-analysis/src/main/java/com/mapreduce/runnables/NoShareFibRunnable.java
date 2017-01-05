package com.mapreduce.runnables;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mapreduce.dataobject.StationInfo;
import com.mapreduce.hw1.q1.EntryPoint;

public class NoShareFibRunnable implements Runnable{

	// Every threads own data structure to update
	private  Map<String, StationInfo> stationTotalTMAXMap = new HashMap<>();

	private int listStart = 0;
	private int listEnd = 0;
	private List<String> fileList = null;

	// This class has to initialized with a task
	private NoShareFibRunnable() {}

	public void updateDataMap(Map<String, StationInfo> dataMap){
		for (String stationId : stationTotalTMAXMap.keySet()){
			if(dataMap.containsKey(stationId)){
				StationInfo stationInfo = dataMap.get(stationId);
				stationInfo.addTMAX(stationTotalTMAXMap.get(stationId).getTMAX());
				stationInfo.addStations(stationTotalTMAXMap.get(stationId).getNoOfStations());				
			}
			else{				
				dataMap.put(stationId, new StationInfo(stationTotalTMAXMap.get(stationId).getTMAX(), stationTotalTMAXMap.get(stationId).getNoOfStations()));
			}
		}
	}

	// initialize with a task
	public NoShareFibRunnable(int start, int end, List<String> fileList){
		listStart = start;
		listEnd = end;
		this.fileList = fileList;
	}

	@Override
	public void run() {
		for(int i=listStart; i<listEnd; i++){
			String[] cols = fileList.get(i).split(",");
			if(cols[2].equals("TMAX")){
				if (stationTotalTMAXMap.containsKey(cols[0])){
					StationInfo stationInfo = stationTotalTMAXMap.get(cols[0]);
					stationInfo.updateStationInfoWithFib(Integer.parseInt(cols[3]));
				}
				else{
					stationTotalTMAXMap.put(cols[0], new StationInfo(Integer.parseInt(cols[3])));
				}
			}
		}

	}

	public static void noSharingExecutor(List<String> fileList) throws InterruptedException{
		int noOfCores = Runtime.getRuntime().availableProcessors();
		int jobsPerThread = fileList.size()/noOfCores;
		double avg = 0;
		long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for(int j=0; j<10; j++){
			int start = 0;
			long startTime = System.currentTimeMillis();
			List<NoShareFibRunnable> listOfRunnables = new ArrayList<>();
			ExecutorService executorService = Executors.newFixedThreadPool(noOfCores);
			for(int i=0; i<noOfCores; i++){
				NoShareFibRunnable tempNoShareRunnable = new NoShareFibRunnable(start, start+jobsPerThread, fileList);
				listOfRunnables.add(tempNoShareRunnable);
				executorService.execute(tempNoShareRunnable);
				start += jobsPerThread;
			}
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

			Map<String, StationInfo> finalResult = new HashMap<>();
			for(NoShareFibRunnable everyRunnable : listOfRunnables){
				everyRunnable.updateDataMap(finalResult);
			}
			long endTime = System.currentTimeMillis();
			max = Math.max(max,  endTime-startTime);
			min = Math.min(min, endTime-startTime);
			avg += endTime-startTime;
		}
		EntryPoint.printMessage(max, min, avg/10, "No Share Lock execution with Fibonacci");
		
	}


}
