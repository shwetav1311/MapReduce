package com.mapreduce.tasktracker;

import java.io.File;
import java.io.IOException;

import com.mapreduce.hdfsutils.GetFile;
import com.mapreduce.hdfsutils.PutFile;
import com.mapreduce.misc.MapReduce.ReducerTaskInfo;
import com.mapreduce.misc.MyFileReader;
import com.mapreduce.misc.MyFileWriter;

class ReduceThread implements Runnable{

	private ReducerTaskInfo info;
	
	public ReduceThread(ReducerTaskInfo info) {
		// TODO Auto-generated constructor stub
		this.info = info;
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		/*
		 * for this one, get all map files 
		 * and create one temp outputfile and upload to hdfs
		 * put it to 
		 * 
		 * 
		 */
		
		String LOCAL_MAP_OUT = "LOCAL_MAP_OUT_"+info.getJobId()+"_"+info.getTaskId();
		
		for(int i=0;i<info.getMapOutputFilesList().size();i++)
		{
			GetFile getFile = new GetFile(info.getMapOutputFiles(i),LOCAL_MAP_OUT+i); 
			Thread thread1 = new Thread(getFile);
			thread1.start();
			try {
				thread1.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		MyFileWriter writer = new MyFileWriter(info.getOutputFile());
		writer.createFile();
		
		
		for(int i=0;i<info.getMapOutputFilesList().size();i++)
		{
			MyFileReader reader  = new MyFileReader(LOCAL_MAP_OUT+i);
			reader.openFile();
			String line="";
			while(line!=null){
				try {
					line = reader.buff_reader.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(line!=null)
				{
					writer.writeline(line);
				}
			}
			reader.closeFile();
			
		}
		
		writer.closeFile();
		
//		System.out.println("Putting reduce file in hdfs "+info.getOutputFile());
		PutFile putFile = new PutFile(info.getOutputFile(), info.getOutputFile());
		Thread thread1 = new Thread(putFile);
		thread1.start();
		try {
			thread1.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		TTrackerDriver.updateReduceStatus(info);		

		
		deleteLocalMapFiles(LOCAL_MAP_OUT, info.getMapOutputFilesList().size());
		deleteLocalReduceFile(info.getOutputFile());
		
		
	}
	
	
	private void deleteLocalReduceFile(String outputFile) {
		// TODO Auto-generated method stub
		boolean status = new File(outputFile).delete();
	}

	public void deleteLocalMapFiles(String fileName,int size)
	{
		for(int i=0;i<size;i++)
		{
			boolean status = new File(fileName+i).delete();
		}
		
		
	}
	
}