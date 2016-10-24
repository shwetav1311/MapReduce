package com.mapreduce.jobtracker;

import java.util.ArrayList;
import java.util.List;

import com.mapreduce.jobtracker.MapInfo.TaskInfo;

public class ReduceInfo {

	public class RedTaskInfo
	{
		List<String> mapOutFiles;
		String outputFile;
		boolean status;
		int taskID;
		
		public RedTaskInfo(List<String> mapOut,boolean st,String out,int task)
		{
			mapOutFiles = mapOut;
			status = st;
			outputFile = out;
			taskID = task;
		}
		
		
	}
	
	public List<RedTaskInfo> tasks; 
	
	public ReduceInfo()
	{
		tasks  = new ArrayList<>();
	}
	
	public void addTask(List<String> mapOut,String out)
	{
		int size = tasks.size();
		RedTaskInfo task = new  RedTaskInfo(mapOut, false, out, size);
		tasks.add(task);
	}
	
	
	/* implementation left */
	
	public boolean isAllTaskCompleted()
	{
		//iterate over all and return a result
		boolean result=tasks.get(0).status; // this takes care of the case where a file is only 1 block
		
		for(int i=1;i<tasks.size();i++)
		{
			result = result && tasks.get(i).status; 
		}
		
		return result;
	}
	
	public void updateStatus(int taskID)
	{
		tasks.get(taskID).status = true;		
	}
	
}
