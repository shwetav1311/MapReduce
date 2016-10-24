package com.mapreduce.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream.GetField;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mapreduce.hdfsutils.GetFile;
import com.mapreduce.jobtracker.IJobTracker;
import com.mapreduce.misc.Constants;
import com.mapreduce.misc.MapReduce.JobStatusRequest;
import com.mapreduce.misc.MapReduce.JobStatusResponse;
import com.mapreduce.misc.MapReduce.JobSubmitRequest;
import com.mapreduce.misc.MapReduce.JobSubmitResponse;

public class ClientDriver {

	public static String mapName;
	public static String reducerName;
	public static String inputFile;
	public static String outputFile;
	public static Integer numReducers;
	
	public static void main(String[] args) {
	// TODO Auto-generated method stub

//		<mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers>
		
		mapName = args[0];
		reducerName  = args[1];
		inputFile = args[2];
		outputFile = args[3];
		numReducers = Integer.parseInt(args[4]);
		
		System.out.print(inputFile);
		
		
		try {
			submitJob();
		} catch (RemoteException | InvalidProtocolBufferException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	private static void submitJob() throws AccessException, RemoteException, NotBoundException, InvalidProtocolBufferException {
		// TODO Auto-generated method stub
		
		
		Registry registry = getRegistry();
		
		if(registry!=null)
		{
			IJobTracker jtStub;
			jtStub=(IJobTracker) registry.lookup(Constants.JOB_TRACKER);  // @Sheshadri : Name kept for binding
			
			JobSubmitRequest.Builder jobSubmitRequest = JobSubmitRequest.newBuilder();
			
			jobSubmitRequest.setInputFile(inputFile);
			jobSubmitRequest.setOutputFile(outputFile);
			jobSubmitRequest.setMapName(mapName);
			jobSubmitRequest.setReducerName(reducerName);
			jobSubmitRequest.setNumReduceTasks(numReducers);
			
			byte[] responseArray = jtStub.jobSubmit(jobSubmitRequest.build().toByteArray());
			
			JobSubmitResponse jobSubmitResponse = JobSubmitResponse.parseFrom(responseArray);
			int jobId =jobSubmitResponse.getJobId();
			
			
			JobStatusRequest.Builder jobStatusRequest = JobStatusRequest.newBuilder();
			jobStatusRequest.setJobId(jobId);
			
			
			if(jobSubmitResponse.getStatus()==Constants.STATUS_FAILED)
			{
				System.out.println("File not found");
				System.exit(0);
			}
			
			while(true)
			{
				byte[] statusResponseArr = jtStub.getJobStatus(jobStatusRequest.build().toByteArray());
				
				JobStatusResponse jobStatusResponse = JobStatusResponse.parseFrom(statusResponseArr);
				
				
				
				if(jobStatusResponse.getJobDone())
				{
					break;
				}else
				{
					
					System.out.println("Total Map Tasks :"+jobStatusResponse.getTotalMapTasks());
					System.out.println("Total Map Tasks Started:"+jobStatusResponse.getNumMapTasksStarted());
					System.out.println("Total Reduce Tasks :"+jobStatusResponse.getTotalReduceTasks());
					System.out.println("Total Reduce Tasks Started :"+jobStatusResponse.getNumReduceTasksStarted());
					
					System.out.println();
					System.out.println();
				}
				
				try {
					Thread.sleep(Constants.JOB_STATUS_FREQ);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			System.out.println("Operation completed");
			
			GetFile getFile = new GetFile(outputFile,outputFile);
			getFile.run();
			
			System.out.println("Have you seen bthis file?? --> "+outputFile);
			
			/**
			 *  Read contents of output file after this.
			 *  Implement hdfs read in misc;
			 */
			
		}
		
		
		
	}
		
		
	private static  Registry getRegistry()
	{
		BufferedReader buff;
		String line="";
		try {
			buff = new BufferedReader(new FileReader(Constants.JT_CONF_FILE));  
			line=buff.readLine();
			buff.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		String tokens[] = line.split(" ");
		
		Registry registry =null;
		try {
			registry = LocateRegistry.getRegistry(tokens[0],Integer.parseInt(tokens[1]));
			
		}catch(RemoteException e)
		{
			e.printStackTrace();
		}
		
		return registry;
	}

}
