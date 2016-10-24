package com.mapreduce.jobtracker;

public class JobResponseUnit {
	public boolean jobDone;
	public int totalMapTasks;
	public int numMapTasksStarted;
	public int totalReduceTasks;
	public int numReduceTasksStarted;
	
	public JobResponseUnit(boolean jobDone, int totalMapTasks, int numMapTasksStarted, int totalReduceTasks,
			int numReduceTasksStarted) 
	{
		super();
		this.jobDone = jobDone;
		this.totalMapTasks = totalMapTasks;
		this.numMapTasksStarted = numMapTasksStarted;
		this.totalReduceTasks = totalReduceTasks;
		this.numReduceTasksStarted = numReduceTasksStarted;
	}
}
