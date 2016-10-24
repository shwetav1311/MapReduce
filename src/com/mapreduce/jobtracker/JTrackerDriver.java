package com.mapreduce.jobtracker;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.namenode.INameNode;
import com.mapreduce.hdfsutils.Hdfs.BlockLocationRequest;
import com.mapreduce.hdfsutils.Hdfs.BlockLocationResponse;
import com.mapreduce.hdfsutils.Hdfs.BlockLocations;
import com.mapreduce.hdfsutils.Hdfs.OpenFileRequest;
import com.mapreduce.hdfsutils.Hdfs.OpenFileResponse;
import com.mapreduce.jobtracker.MapInfo.TaskInfo;
import com.mapreduce.jobtracker.ReduceInfo.RedTaskInfo;
import com.mapreduce.misc.Constants;
import com.mapreduce.misc.MapReduce.DataNodeLocation;
import com.mapreduce.misc.MapReduce.HeartBeatRequest;
import com.mapreduce.misc.MapReduce.HeartBeatResponse;
import com.mapreduce.misc.MapReduce.JobStatusRequest;
import com.mapreduce.misc.MapReduce.JobStatusResponse;
import com.mapreduce.misc.MapReduce.JobSubmitRequest;
import com.mapreduce.misc.MapReduce.JobSubmitResponse;
import com.mapreduce.misc.MapReduce.MapTaskInfo;
import com.mapreduce.misc.MapReduce.MapTaskStatus;
import com.mapreduce.misc.MapReduce.ReduceTaskStatus;
import com.mapreduce.misc.MapReduce.ReducerTaskInfo;
import com.mapreduce.misc.MyFileReader;
/**
 * 
 * @author master
 *	Implementation details
 *	1. HashMap for storing jobs (jobId(int), jobSubmitReq)
 *	2. HashMap for job submit response (jobId(int), jobSubmitRes)
 *	3. HashMap for JobMapInfo (jobId(int), MapInfo)
 *  4. HashMap for JobReduceInfo (jobId(int), ReduceInfo) 
 */




public class JTrackerDriver implements IJobTracker {


	
	public static HashMap<Integer, JobSubmitRequest> jobRequests;//this contains mapper name
	public static HashMap<Integer,JobResponseUnit> jobResponseUnitMap;
	public static HashMap<Integer, JobStatusResponse> jobStatus; // this is not needed
	
	public static HashMap<Integer, MapInfo> jobMapInfo;
	public static HashMap<Integer, ReduceInfo> jobReduceInfo;
	
	public static GenericQueue<MapQueueUnit> mapQueue;
	public static GenericQueue<ReduceQueueUnit> reduceQueue;

	public static HashMap<Integer, List<String>> mapOutputFiles;
	public static HashMap<Integer, List<String>> reduceOutputFiles; //this has to be given by server only	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("java.security.policy","./security.policy");
		//set the security manager
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		/**initialize data structures **/
		initializeDataStructures();
		
        /**call bind to registry **/
        bindToRegistry();
        
     
	}
	
	
	

	@Override
	public byte[] jobSubmit(byte[] jobSubmitRequest) {
		// TODO Auto-generated method stub
		System.out.println("Job Submit got called");
		int jobID = generateJobID();
		
		System.out.println("JoB id is "+jobID);
		
//		if(true)
//			return null;
		
		try {
			JobSubmitRequest jSubReqObj = JobSubmitRequest.parseFrom(jobSubmitRequest);
			
			
			
			jobRequests.put(jobID, jSubReqObj); // job requests has a name of mapper, reducer, number of mapper,redcucer everything
									
			/**Stores the map output  files generated for a given job ID **/
			mapOutputFiles.put(jobID, new ArrayList<String>());
			
			/**next get the number of blocks from name node server**/
			List<BlockLocations> blockLocations = getBlockLocations(jSubReqObj.getInputFile());
			
			if(blockLocations == null)
			{
				JobSubmitResponse.Builder jobStatusResObj = JobSubmitResponse.newBuilder();
				jobStatusResObj.setJobId(jobID);
				jobStatusResObj.setStatus(Constants.STATUS_FAILED); //STATUS HARD CODED
				return jobStatusResObj.build().toByteArray();
			}
			
//			System.out.println("What's up doc? "+blockLocations);
			jobMapInfo.put(jobID, new MapInfo(blockLocations));
			
			/**Create a jobResponsenit Map and keep it ready for sending responses**/
			createJobResponse(jobID,blockLocations.size(),jSubReqObj.getNumReduceTasks());
			
			/**Create the name of outputFiles **/
			createReduceOutputFiles(jobID,jSubReqObj.getNumReduceTasks());
			
			/**populates the task queue for mapper **/
			populateMapQueue(jobID,jSubReqObj.getMapName());
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		JobSubmitResponse.Builder jobStatusResObj = JobSubmitResponse.newBuilder();
		jobStatusResObj.setJobId(jobID);
		jobStatusResObj.setStatus(Constants.STATUS_SUCCESS); //STATUS HARD CODED
		
		/**have to formulate a response **/
		return jobStatusResObj.build().toByteArray();
	}

	
	
	/**Creates job response data unit **/
	static void createJobResponse(int localjobID, int noOfMappers, int numOfReducers) {
		 
		JobResponseUnit jobResUnitObj = new JobResponseUnit(false, noOfMappers, 0, numOfReducers, 0);
		jobResponseUnitMap.put(localjobID, jobResUnitObj);
	}



	/** Creates reduce output files **/
	static void createReduceOutputFiles(int jobIDArgs,int numReduceTasks) {
		
		List<String> reduceOutputFileNames = new ArrayList<String>();
		
		for(int i=0;i<numReduceTasks;i++)
		{
			reduceOutputFileNames.add(Constants.REDUCE_FILE+ "_"+jobIDArgs+"_"+i);
		}
		
		reduceOutputFiles.put(jobIDArgs, reduceOutputFileNames);
	}




	/**This method populates the queue of the mapper **/
	void populateMapQueue(int jobIDArgs,String mapperName) {
		
		int noOfItemsAdded = 0;
		System.out.println("Entered Job ID is "+jobIDArgs);
		MapInfo mapInfoObj = jobMapInfo.get(jobIDArgs);
		
		List<TaskInfo> mapTasks= mapInfoObj.tasks;
		
		Iterator<TaskInfo> itr = mapTasks.iterator();
		
		while(itr.hasNext())
		{
			TaskInfo taskObj= itr.next();
			MapQueueUnit mapQueueObject = new MapQueueUnit(jobIDArgs,taskObj.taskID,mapperName,taskObj.blockLocations);
			mapQueue.enqueue(mapQueueObject);
			noOfItemsAdded++;
		}
		
	
		System.out.println("The job ID is "+jobIDArgs+" The number of items added are : "+noOfItemsAdded);
		
	}



	/**JOB STATUS **/
	@Override
	public byte[] getJobStatus(byte[] jobStatusRequestByte) {
		// TODO Auto-generated method stub
//		System.out.println("getJobStatus got called");
		
		JobResponseUnit jobResponseObj = null;
		
		JobStatusResponse.Builder jStatusResObj = JobStatusResponse.newBuilder();
		
		try
		{
			
			JobStatusRequest jStatusReqObj = JobStatusRequest.parseFrom(jobStatusRequestByte) ; //parse from incoming byte
			
			/** DO A GET FROM THE JOB STATUS HASH MAP **/
			
			
			jobResponseObj =  jobResponseUnitMap.get(jStatusReqObj.getJobId());
			
			jStatusResObj.setStatus(Constants.STATUS_SUCCESS);// STATUS HARD CODE
			jStatusResObj.setJobDone(jobResponseObj.jobDone);
			jStatusResObj.setTotalMapTasks(jobResponseObj.totalMapTasks);
			jStatusResObj.setNumMapTasksStarted(jobResponseObj.numMapTasksStarted);
			jStatusResObj.setTotalReduceTasks(jobResponseObj.totalReduceTasks);
			jStatusResObj.setNumReduceTasksStarted(jobResponseObj.numReduceTasksStarted);
		}
		catch (InvalidProtocolBufferException e) {
			 
			System.out.println("Invlaid proto exception in class JTRACKERDRIVER method getJobStatus");
			e.printStackTrace();
		}
		
		return jStatusResObj.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] heartBeatRequestByte) {
		
		/**Heart beat is called by the task trackers every second, they send the number of map threads and reduce thread slots free **/
		
//		System.out.println("HeartBeat got called");
		
//		if(true)
//			return null;
		
		HeartBeatResponse.Builder hBeatResponseObj = null;
		
		try
		{
			HeartBeatRequest hBeatReqObj = HeartBeatRequest.parseFrom(heartBeatRequestByte);
			
			hBeatResponseObj= processHeatBeatRequest(hBeatReqObj);
			
		}
		catch (InvalidProtocolBufferException e) {
			
			System.out.println("Invlaid proto exception in class JTRACKERDRIVER method heartBeat");
			e.printStackTrace();
		}
		
		return hBeatResponseObj.build().toByteArray();
	}
	
	/**processes the incoming heart beat request **/
	static HeartBeatResponse.Builder processHeatBeatRequest(HeartBeatRequest hBeatReqObj)
	{
		/**incoming from a task tracker **/
		int numOfMapSlot = hBeatReqObj.getNumMapSlotsFree();
		int numOfReduceSlot = hBeatReqObj.getNumReduceSlotsFree();
		List<MapTaskStatus> mapTasksStatus = hBeatReqObj.getMapStatusList();
		List<ReduceTaskStatus> reduceTasksStatus = hBeatReqObj.getReduceStatusList();
//		DataNodeLocation tTrackerLoc = hBeatReqObj.getLocations(); //NOT NEEDED I GUESS
		
		
		updateMapTasksStatus(mapTasksStatus);
		
		updateReduceTasksStatus(reduceTasksStatus);
		
		HeartBeatResponse.Builder hBeatResponseObj = HeartBeatResponse.newBuilder();
		
		hBeatResponseObj.setStatus(Constants.STATUS_SUCCESS); // STATUS HARD CODED
		
		if(mapQueue.hasItems()) //means map queue has jobs to schedule
		{
			if(numOfMapSlot>0)
			{
				//then schedule a task until the queue gets over or the threads get lesser
				int numOfMapThreads = numOfMapSlot;
				
//				int index = 0;
				
				while(numOfMapThreads>0 && mapQueue.hasItems())
				{
					MapQueueUnit  mapQueueItem = mapQueue.dequeue(); //this handles queue condition
					
					MapTaskInfo.Builder mapTaskInfoObj = MapTaskInfo.newBuilder();
//					System.out.println("This is the mapQueue item "+mapQueueItem.toString());
					mapTaskInfoObj.setJobId(mapQueueItem.jobID);
					mapTaskInfoObj.setTaskId(mapQueueItem.taskID);
					mapTaskInfoObj.setMapName(mapQueueItem.mapName);
					
					com.mapreduce.misc.MapReduce.BlockLocations.Builder blockLocationObj = com.mapreduce.misc.MapReduce.BlockLocations.newBuilder();
					blockLocationObj.setBlockNumber(mapQueueItem.inputBlock.getBlockNumber());//blocklocation => (blockNumber + datanodelocations)
					
					List<com.mapreduce.hdfsutils.Hdfs.DataNodeLocation> dataNodeLocations = mapQueueItem.inputBlock.getLocationsList();
					
					for(int k=0;k<dataNodeLocations.size();k++)
					{
						DataNodeLocation.Builder miscDataNodeLocation = DataNodeLocation.newBuilder();
						miscDataNodeLocation.setIp(dataNodeLocations.get(k).getIp());
						miscDataNodeLocation.setPort(dataNodeLocations.get(k).getPort());
						
						blockLocationObj.addLocations(miscDataNodeLocation);
					}
					
					mapTaskInfoObj.addInputBlocks(blockLocationObj);
				
					hBeatResponseObj.addMapTasks(mapTaskInfoObj); //because this is a repeated field in proto
					
					numOfMapThreads--;
//					index++;
					//increment that job status structure
					jobResponseUnitMap.get(mapQueueItem.jobID).numMapTasksStarted++;
				}
//				index = 0;
				
			}
		}// map queue ends here
		
		if(reduceQueue.hasItems())
		{
			if(numOfReduceSlot>0)
			{
				int numOfReduceThreads= numOfReduceSlot;
				
//				int index = 0;
				
				while(numOfReduceThreads>0 && reduceQueue.hasItems())
				{
					ReduceQueueUnit redQueueItem = reduceQueue.dequeue();//this takes care of the queue condition
					
					ReducerTaskInfo.Builder reducerTaskObj = ReducerTaskInfo.newBuilder();
					
					reducerTaskObj.setJobId(redQueueItem.jobID);
					reducerTaskObj.setTaskId(redQueueItem.taskID);
					reducerTaskObj.setOutputFile(redQueueItem.outputFile);
					reducerTaskObj.setReducerName(redQueueItem.reducerName);
					reducerTaskObj.addAllMapOutputFiles(redQueueItem.mapOutputFiles);
					
					hBeatResponseObj.addReduceTasks(reducerTaskObj);
					
					numOfReduceThreads--;
//					index++;
					//increment the status of the jobstatus response
					jobResponseUnitMap.get(redQueueItem.jobID).numReduceTasksStarted++;
				}
			}
		}
		
		return hBeatResponseObj;
	}


	/**Updates the statuses of the reduce tasks of a given job **/
	static void updateReduceTasksStatus(List<ReduceTaskStatus> reduceTasksStatus) 
	{
		Iterator<ReduceTaskStatus> itr = reduceTasksStatus.iterator();
		 while(itr.hasNext())
		 {
			 ReduceTaskStatus reduceTaskStObj = itr.next();
			 
			 if(reduceTaskStObj.getJobId()<0) //as in it has not been assigned a job yet
			 {
				 System.out.println("No reduce tasks assigned yet :(");
				 continue;
			 }
			 //else job has been assigned update it's completion status
			 if(reduceTaskStObj.hasTaskCompleted()==true)
			 {
				 //get the task ID of the job and update the completion status to TRUE
				 int taskID = reduceTaskStObj.getTaskId();
				 int localJobID = reduceTaskStObj.getJobId();
				 
				 jobReduceInfo.get(localJobID).updateStatus(taskID);
//				 mapOutputFiles.get(localJobID).add(mapTaskStObj.getMapOutputFile()); //added output File
				 
				 if(jobReduceInfo.get(localJobID).isAllTaskCompleted()==true)
				 {
					 //If all reduce tasks are completed for a given job ID, then we need to 
					 //concatenate both the files
					 jobResponseUnitMap.get(localJobID).jobDone = true;
					 concatenateReduceFiles(localJobID);
				 }
			 }
			 
		 }
		
	}




	static void concatenateReduceFiles(int localJobID) {
	
		//The file names are available to the Job Tracker, it just has to write it as a single file
		WriteOutputFile outputFileObj = new WriteOutputFile(jobRequests.get(localJobID).getOutputFile(),reduceOutputFiles.get(localJobID) );
		outputFileObj.writeIntoHDFS();
		System.out.println("Have you seen this file?? -->"+ jobRequests.get(localJobID).getOutputFile());
		
	}




	/**Updates the statuses of the map tasks of a given job **/
	static void updateMapTasksStatus(List<MapTaskStatus> mapTasksStatus)
	{
		Iterator<MapTaskStatus> itr = mapTasksStatus.iterator();
		 while(itr.hasNext())
		 {
			 MapTaskStatus mapTaskStObj = itr.next();
			 
			 if(mapTaskStObj.getJobId()<0) //as in it has not been assigned a job yet
			 {
				 System.out.println("No task alloted yet! :( ");
				 continue;
			 }
			 //else job has been assigned update it's completion status
			 if(mapTaskStObj.hasTaskCompleted()==true)
			 {
				 //get the task ID of the job and update the completion status to TRUE
				 int taskID = mapTaskStObj.getTaskId();
				 int localJobID = mapTaskStObj.getJobId();
				 
//				 //code changed here Shesh 12:38AM
//				 MapInfo localMapInfo = jobMapInfo.get(localJobID);
//				 if(localMapInfo.tasks.get(taskID).status==true)
//					 continue;
				 
				 jobMapInfo.get(localJobID).updateStatus(taskID); //update the status of completion for that task to be true
				 
//				 System.out.println("File sent by Task Tracker is "+mapTaskStObj.getMapOutputFile());
				 
				 mapOutputFiles.get(localJobID).add(mapTaskStObj.getMapOutputFile()); //added output File for the completed task
				 
//				 System.out.println("This file is sent by Task Tracker "+mapOutputFiles.toString());
				 if(jobMapInfo.get(localJobID).isAllTaskCompleted()==true)
				 {
					 //schedule reduce task for that job
					 //populate a queue of reducer for that task
					 populateReduceQueue(localJobID);
				 }
			 }
			 
		 }		
	}




	static void populateReduceQueue(int localJobID) {
	
		System.out.println("Populating Reduce Queue");
		//first update the jobReduceInfo structure
		ReduceInfo redInfoObj = new ReduceInfo();
		//need number of reducers sent from the client
		
		int noOfReducers = jobRequests.get(localJobID).getNumReduceTasks(); //contains job submit request, sent by client at beginning
		
		String reducerName = jobRequests.get(localJobID).getReducerName();
		
		List<String> myLines = mapOutputFiles.get(localJobID);// MAP OUTPUT FILES ARE GENERATED AFTER MAPPER IS DONE
		
		int size = myLines.size();
		
		int quotient = size / noOfReducers;
		
		int remainder = size - (noOfReducers*quotient);
		
		int counter=0;
		for(int i=0;i<noOfReducers;i++)
		{
				List<String> divLines = new ArrayList<>();
				for(int j=0;j<quotient;j++)
				{
					divLines.add(myLines.get(counter));
					counter++;
				}
				
				if(i==noOfReducers-1)
				{
					for(int j=0;j<remainder;j++)
					{
						divLines.add(myLines.get(counter));
						counter++;
					}
				}
				
//				System.out.println()
				
				redInfoObj.addTask(divLines, reduceOutputFiles.get(localJobID).get(i));//no of map output files, per reducer and its outputfile name
				
		}
		jobReduceInfo.put(localJobID, redInfoObj);

//		System.out.println("Check here "+jobReduceInfo.toString());
		
		/**now redInfo Object has a job ID and number of tasks in it
		 * we have to put it into the reduceQueue
		 *  **/
		
		
		ReduceInfo reduceInfoObj = jobReduceInfo.get(localJobID);
		
//		System.out.println("Finally "+reduceInfoObj.tasks);
		
		List<RedTaskInfo> redTasks= reduceInfoObj.tasks;
		
		Iterator<RedTaskInfo> itr = redTasks.iterator();
		
		int noOfTaksInQueue = 0;
		
		while(itr.hasNext())
		{
			RedTaskInfo taskObj= itr.next();
			ReduceQueueUnit redQueueObject = new ReduceQueueUnit(localJobID, taskObj.taskID, reducerName, taskObj.mapOutFiles, taskObj.outputFile);
			reduceQueue.enqueue(redQueueObject);
			noOfTaksInQueue++;
		}
		
		System.out.println("No of reducer tasks put into queue are "+noOfTaksInQueue);
		
	}




	/**Get block Locations from the name node server **/
	static List<BlockLocations> getBlockLocations(String fileName)
	{
		
		
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		
		openFileReqObj.setFileName(fileName); //filename is sent in the job request		
		openFileReqObj.setForRead(true);
		
		byte[] responseArray;
		List<BlockLocations> blockLocs= null;
		
		try 
		{
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			nameStub=(INameNode) registry.lookup(Constants.NAME_NODE);
			
			responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
			
			try
			{
				OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
//				System.out.println(responseObj);
				if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND)
				{
					System.out.println("File not found fatal error, Exception in JT CLASS: getBlockNums functions");
					return null;
//					System.exit(0);
				}
				
				List<Integer> blockNums = responseObj.getBlockNumsList();
				BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
				
//				System.out.println(blockNums);
				/**Now perform Read Block Request  from all the blockNums**/
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					System.out.println("Remote Exception in JT CLASS: getBlockNums functions");
					e.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
//				System.out.println(blockLocResObj.toString());
				
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				blockLocs =  blockLocResObj.getBlockLocationsList();
				
			} 
			
			catch (InvalidProtocolBufferException e)
			{
				System.out.println("Invalid proto Exception in JT CLASS: getBlockNums functions");
				e.printStackTrace();
			}	
		
		} 
		
		catch (NotBoundException e)
		{
			System.out.println(" Not bound Exception in JT CLASS: getBlockNums functions ");
			e.printStackTrace();
		} 
		catch (RemoteException e) 
		{
			System.out.println(" Remote Exception in JT CLASS: getBlockNums functions ");
			e.printStackTrace();
			
		}
		
		System.out.println("Number of blocks are  "+blockLocs.size());
		return blockLocs;
		
	}
	
	/**Generate job ID **/
//	static void generateJobID()
//	{
//		jobID++;
//	}
	
	/**bind to registry method **/
	static void bindToRegistry()
	{
		System.setProperty("java.rmi.server.hostname",Constants.JOB_TRACKER_IP);
		JTrackerDriver obj = new JTrackerDriver();
		try {
			
			Registry register=LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(obj,Registry.REGISTRY_PORT);
			try {
				register.bind(Constants.JOB_TRACKER, stub);
				
				System.out.println("JOB TRACKER started succesfully");
			} catch (AlreadyBoundException e) {
				// TODO Auto-generated catch block
				System.out.println("JOB TRACKER failed to bind");
				e.printStackTrace();
			}
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}


	/**Initialize all data structures **/
	static void initializeDataStructures()
	{
		jobRequests = new HashMap<>();
		jobStatus = new HashMap<Integer, JobStatusResponse>();
		jobMapInfo = new HashMap<>();
		jobReduceInfo = new HashMap<>();
		mapOutputFiles = new HashMap<>();
		reduceOutputFiles = new HashMap<>();
		mapQueue = new GenericQueue<MapQueueUnit>();
		reduceQueue = new GenericQueue<ReduceQueueUnit>();
		jobResponseUnitMap = new HashMap<>();
	}
	
	
	public static synchronized int generateJobID()
	{
		MyFileReader myReader = new MyFileReader(Constants.JOB_ID_FILE);
		myReader.openFile();
		Integer value=0;
		try {
			String line = myReader.buff_reader.readLine();
			value = Integer.parseInt(line);
			value++;
			
			PrintWriter pw;
			
			pw = new PrintWriter(new FileWriter(Constants.JOB_ID_FILE));
		    pw.write(value.toString());
	        pw.close();
			
		} catch (IOException e) {
			 
			e.printStackTrace();
		}
		myReader.closeFile();
		
		return value;
	}
}
