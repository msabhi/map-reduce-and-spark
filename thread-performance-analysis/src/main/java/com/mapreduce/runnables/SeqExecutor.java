package com.mapreduce.runnables;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import com.mapreduce.dataobject.StationInfo;
import com.mapreduce.hw1.q1.EntryPoint;
import com.mapreduce.runnables.CoarseLockRunnable;

public class SeqExecutor {

	// Shared data strucuture to maintain the word count
	private static Map<String, StationInfo> stationTotalTMAXMap = new HashMap<>();

	public static void execute(List<String> fileLines){
		double avg = 0;
		long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
		for(int j=0; j<10; j++){
			stationTotalTMAXMap = new HashMap<>();
			long startTime = System.currentTimeMillis();
			for(String line : fileLines){
				String[] cols = line.split(",");
				if(cols[2].trim().equals("TMAX")){
					if(stationTotalTMAXMap.containsKey(cols[0])){
						StationInfo stationInfo = stationTotalTMAXMap.get(cols[0]);
						stationInfo.updateStationInfo(Integer.parseInt(cols[3]));
					}
					else{
						stationTotalTMAXMap.put(cols[0], new StationInfo(Integer.parseInt(cols[3])));
					}
				}
			}
			long endTime = System.currentTimeMillis();
			max = Math.max(max,  endTime-startTime);
			min = Math.min(min, endTime-startTime);
			avg += endTime-startTime;
		}
		EntryPoint.printMessage(max, min, avg/10, "Sequential execution without Fibonacci");
	}

}
