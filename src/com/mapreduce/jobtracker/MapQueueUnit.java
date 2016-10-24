package com.mapreduce.jobtracker;

import com.mapreduce.hdfsutils.Hdfs.BlockLocations;

public class MapQueueUnit {

	int jobID;
	int taskID;
	String mapName;
	BlockLocations inputBlock;
	
	public MapQueueUnit(int jobIDArgs,int taskIDArgs,String mapNameArgs,BlockLocations inputBlockArgs) {
		 
		jobID = jobIDArgs;
		taskID = taskIDArgs;
		mapName = mapNameArgs;
		inputBlock = inputBlockArgs;
	}

	@Override
	public String toString() {
		return "MapQueueUnit [jobID=" + jobID + ", taskID=" + taskID + ", mapName=" + mapName + ", inputBlock="
				+ inputBlock + "]";
	}
}
