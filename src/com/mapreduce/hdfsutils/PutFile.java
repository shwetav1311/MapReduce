package com.mapreduce.hdfsutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.datanode.IDataNode;
import com.hdfs.namenode.INameNode;
import com.mapreduce.hdfsutils.Hdfs.AssignBlockRequest;
import com.mapreduce.hdfsutils.Hdfs.AssignBlockResponse;
import com.mapreduce.hdfsutils.Hdfs.BlockLocations;
import com.mapreduce.hdfsutils.Hdfs.CloseFileRequest;
import com.mapreduce.hdfsutils.Hdfs.CloseFileResponse;
import com.mapreduce.hdfsutils.Hdfs.DataNodeLocation;
import com.mapreduce.hdfsutils.Hdfs.OpenFileRequest;
import com.mapreduce.hdfsutils.Hdfs.OpenFileResponse;
import com.mapreduce.hdfsutils.Hdfs.WriteBlockRequest;
import com.mapreduce.misc.Constants;

public class PutFile implements Runnable {

	public String fileName;
	public long FILESIZE;
	public FileInputStream fis;
	public String threadName;
	private Thread t;

	public PutFile(String threadNameArgs,String fileNameArgs) {
		super();
		threadName = threadNameArgs;
		fileName = fileNameArgs;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		openFilePut(); //PUT to HDFS
	}

	private void openFilePut() {
		// TODO Auto-generated method stub
		
		int fileHandle;
		byte[] responseArray;
		
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);
		openFileReqObj.setForRead(false);		
		
		try 
		{			
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			int status;
			
				try 
				{
					nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);
					responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
					
					/**The response Array will contain the FileHandle status and the block numbers **/
					
					OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
					
					fileHandle = responseObj.getHandle();
//					System.out.println("The file handle is "+fileHandle);
					
					status = responseObj.getStatus();
					if(status==Constants.STATUS_FAILED )//status failed change it
					{
						System.out.println("Fatal Error!");
						System.exit(0);
					}
					else if(status==Constants.STATUS_NOT_FOUND)
					{
						System.out.println("Duplicate File "+fileName);
						System.exit(0);
					}
					
					AssignBlockRequest.Builder assgnBlockReqObj = AssignBlockRequest.newBuilder(); 
					
					
					/**required variables **/

					
					int offset=0;
					
					/**calculate block size **/
					int no_of_blocks=getNumberOfBlocks(fileName);					
					try {
						/**open the input stream **/
						fis = new FileInputStream(fileName);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

//					System.out.println("No of blocks are "+no_of_blocks);
					if(no_of_blocks==0)
						no_of_blocks=1;
					
					/**FOR LOOP STARTS HERE **/
					for(int i=0;i<no_of_blocks;i++)
					{
						WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
						AssignBlockResponse assignResponseObj ;
						BlockLocations blkLocation ;
						List<DataNodeLocation> dataNodeLocations;
						DataNodeLocation dataNode;
						/**need to call assign block and write blocks **/
						
						assgnBlockReqObj.setHandle(fileHandle);
						
						/**Calling assign block **/
						responseArray = nameStub.assignBlock(assgnBlockReqObj.build().toByteArray());
						
						assignResponseObj = AssignBlockResponse.parseFrom(responseArray);
						
						status = assignResponseObj.getStatus();
						if(status==Constants.STATUS_FAILED)
						{
							System.out.println("Fatal Error!");
							System.exit(0);
						}
						
						blkLocation = assignResponseObj.getNewBlock();
						
						int blockNumber = blkLocation.getBlockNumber();
//						System.out.println("Block number retured is "+blockNumber);
						
						dataNodeLocations = blkLocation.getLocationsList();
						
						dataNode = dataNodeLocations.get(0);
//						dataNodeLocations.remove(0);
						
						
						Registry registry2=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

						System.out.println("PutFile Datanodes : " + dataNode);
						IDataNode dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
//						dataStub.readBlock(null);
						
//						System.out.println("Control enters here");
						/**read 32MB from file, send it as bytes, this fills in the byteArray**/
						
						byte[] byteArray = read32MBfromFile(offset,fileName);
						offset=offset+(int)Constants.BLOCK_SIZE;
						
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						writeBlockObj.setBlockInfo(blkLocation);
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
												
					}
					
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					
					try {
						/**Close the input Stream **/
						fis.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				catch (NotBoundException | InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					System.out.println("Could not find NameNode");
					e.printStackTrace();
				}
				
			
		}catch (RemoteException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
		}		
		
	}

	/**Read 32MB size of data from the provided input file **/
	public byte[] read32MBfromFile(int offset,String fileName)
	{
		
//		System.out.println("offset is "+offset);

		
		BufferedReader breader = null;
		try {
			breader = new BufferedReader(new FileReader(fileName) );
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		
		
		int bytesToBeRead = (int)Constants.BLOCK_SIZE;
		
		int limit =offset+(int)Constants.BLOCK_SIZE; 
		
		if(limit >= (int) FILESIZE)
		{
			bytesToBeRead = (int)FILESIZE - offset;
		}
		else
		{
			bytesToBeRead = (int)Constants.BLOCK_SIZE;			
		}
		
		char[] newCharArray = new char[bytesToBeRead];
		
		try {
			breader.skip(offset);
			breader.read(newCharArray, 0, bytesToBeRead);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		try {
			breader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		System.out.println("The new char array is "+newCharArray.length);
		return new String(newCharArray).getBytes(StandardCharsets.UTF_8);
		
	}

	
	
	/**Returns the number of blocks the file to which the file gets divided into **/
	public int getNumberOfBlocks(String fileName)
	{
		File inputFile = new File(fileName);
		if(!inputFile.exists())
		{
			System.out.println("File Does not exist");
			System.exit(0);
		}
		
		long fileSize = inputFile.length();
		FILESIZE=inputFile.length();
		double noOfBlocks = Math.ceil((double)fileSize*1.0/(double)Constants.BLOCK_SIZE*1.0);
		
//		System.out.println("The length of the file is "+fileSize+ " Number of blocks are "+(int)noOfBlocks);
		
		return (int)noOfBlocks;
	}
	
	public void start ()
	   {
//	      System.out.println("Starting " +  threadName );
	      if (t == null)
	      {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
	   }
	
}
