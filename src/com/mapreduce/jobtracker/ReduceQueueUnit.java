package com.mapreduce.jobtracker;

import java.util.List;

public class ReduceQueueUnit {

	int jobID;
	int taskID;
	String reducerName;
	List<String> mapOutputFiles;
	String outputFile;
	
	
	
	public ReduceQueueUnit(int jobID, int taskID, String reducerName, List<String> mapOutputFiles, String outputFile) {
		super();
		this.jobID = jobID;
		this.taskID = taskID;
		this.reducerName = reducerName;
		this.mapOutputFiles = mapOutputFiles;
		this.outputFile = outputFile;
	}
	
}
