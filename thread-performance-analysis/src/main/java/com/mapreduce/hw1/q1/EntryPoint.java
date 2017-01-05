package com.mapreduce.hw1.q1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.mapreduce.runnables.CoarseLockFibRunnable;
import com.mapreduce.runnables.CoarseLockRunnable;
import com.mapreduce.runnables.FineLockFibRunnable;
import com.mapreduce.runnables.FineLockRunnable;
import com.mapreduce.runnables.NoLockFibRunnable;
import com.mapreduce.runnables.NoLockRunnable;
import com.mapreduce.runnables.NoShareFibRunnable;
import com.mapreduce.runnables.NoShareRunnable;
import com.mapreduce.runnables.SeqExecutor;
import com.mapreduce.runnables.SeqFibExecutor;

public class EntryPoint {
	
	//Execute all variations of locks to capture the statistics on the console
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, InterruptedException{
		if(args.length==0 || args.length > 1){
			System.out.println("Usage : pgm <input_file>");
			System.exit(0);
		}
		String input_file = args[0];		
		List<String> fileList = getLines(input_file);
		SeqFibExecutor.execute(fileList);
		SeqExecutor.execute(fileList);
		NoShareFibRunnable.noSharingExecutor(fileList);
		NoShareRunnable.noSharingExecutor(fileList);
		CoarseLockFibRunnable.coarseLockExecutor(fileList);
		CoarseLockRunnable.coarseLockExecutor(fileList);
		FineLockFibRunnable.fineLockExecutor(fileList);
		FineLockRunnable.fineLockExecutor(fileList);
		NoLockFibRunnable.noLockExecutor(fileList);
		NoLockRunnable.noLockExecutor(fileList);
	}
	
	//Function to read the file into list of lines
	public static List<String> getLines(String input_file) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(input_file));
		List<String> fileLinesList = new ArrayList<String>();
		String everyLine = null;
		while((everyLine = br.readLine()) != null){
			fileLinesList.add(everyLine);
		}
		return fileLinesList;
	}
	
	//Function to compute Fibonacci
	public static int doFibonacci(int val){
		if(val == 1) return 1;
		if(val == 0) return 0;
		return doFibonacci(val-1) + doFibonacci(val-2);
	}
	
	//Function to print the message to the console
	public static void printMessage(long max, long min, double avg, String msgType){
		System.out.println("=======================================================");
		System.out.println(msgType);
		System.out.println("MIN => " + min);
		System.out.println("MAX => " + max);
		System.out.println("AVG => " + avg);
		System.out.println("=======================================================");
	}
	

}
