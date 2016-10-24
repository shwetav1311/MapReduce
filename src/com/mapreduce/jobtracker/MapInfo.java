package com.mapreduce.jobtracker;

import java.util.ArrayList;
import java.util.List;

import com.mapreduce.hdfsutils.Hdfs.BlockLocations;


public class MapInfo {

	
	public class TaskInfo
	{
		boolean status;
		int taskID;
		BlockLocations blockLocations;
		
		public TaskInfo(BlockLocations blk,boolean st,int task)
		{
			blockLocations = blk;
			status = st;
			taskID = task;			
		}
	}
	
	List<TaskInfo> tasks;  // index is taskID
	
	public MapInfo(List<BlockLocations> blocks)
	{
		tasks  = new ArrayList<>();
		for(int i=0;i<blocks.size();i++)
		{
			TaskInfo task  = new TaskInfo(blocks.get(i),false,i);
			tasks.add(task);
		}
	}
	
	/*** implementation left */
	
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
