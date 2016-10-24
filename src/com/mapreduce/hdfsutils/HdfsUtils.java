package com.mapreduce.hdfsutils;

import java.nio.charset.StandardCharsets;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.datanode.IDataNode;
import com.hdfs.namenode.INameNode;
import com.mapreduce.hdfsutils.Hdfs.BlockLocationRequest;
import com.mapreduce.hdfsutils.Hdfs.BlockLocationResponse;
import com.mapreduce.hdfsutils.Hdfs.BlockLocations;
import com.mapreduce.hdfsutils.Hdfs.DataNodeLocation;
import com.mapreduce.hdfsutils.Hdfs.OpenFileRequest;
import com.mapreduce.hdfsutils.Hdfs.OpenFileResponse;
import com.mapreduce.hdfsutils.Hdfs.ReadBlockRequest;
import com.mapreduce.hdfsutils.Hdfs.ReadBlockResponse;
import com.mapreduce.misc.Constants;


public class HdfsUtils {

	
		
	public static void performFileGet(String fileName)
	{
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);		
		openFileReqObj.setForRead(true);
		
		
		FileWriterClass fileWriteObj = new FileWriterClass(Constants.OUTPUT_FILE+fileName);
		fileWriteObj.createFile();
		
		byte[] responseArray;
		
		try {
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			nameStub=(INameNode) registry.lookup(Constants.NAME_NODE);
			responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
			
			try {
				OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
				if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND)
				{
					System.out.println("File not found fatal error");
					System.exit(0);
				}
				
				List<Integer> blockNums = responseObj.getBlockNumsList();
				BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
				
//				System.out.println(blockNums);
				/**Now perform Read Block Request  from all the blockNums**/
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
//				System.out.println(blockLocResObj.toString());
				
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				List<BlockLocations> blockLocations =  blockLocResObj.getBlockLocationsList();
				
				
				for(int i=0;i<blockLocations.size();i++)
				{
					BlockLocations thisBlock = blockLocations.get(i);
					
					int blockNumber = thisBlock.getBlockNumber();					
					List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();
					
					if(dataNodes==null || dataNodes.size()==0)
					{
						System.out.println("All nodes are down :( ");
						System.exit(0);
					}
					
					int dataNodeCounter=0;
					
					DataNodeLocation thisDataNode = null;//dataNodes.get(dataNodeCounter);					
					String ip;// = thisDataNode.getIp();
					int port ; //= thisDataNode.getPort();
					
					
					IDataNode dataStub=null;
					
					boolean gotDataNodeFlag=false;
					
					do
					{
						try
						{
							thisDataNode = dataNodes.get(dataNodeCounter);
							ip = thisDataNode.getIp();
							port = thisDataNode.getPort();
														
							Registry registry2=LocateRegistry.getRegistry(ip,port);					
							dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
							gotDataNodeFlag=true;
						}
						catch (RemoteException e) {
							
							gotDataNodeFlag=false;
//							System.out.println("Remote Exception");
							dataNodeCounter++;
						} 
					}					
					while(gotDataNodeFlag==false && dataNodeCounter<dataNodes.size());
					
					if(dataNodeCounter == dataNodes.size())
					{
						System.out.println("All data nodes are down :( ");
						System.exit(0);
					}
					

					/**Construct Read block request **/
					ReadBlockRequest.Builder readBlockReqObj = ReadBlockRequest.newBuilder();
					readBlockReqObj.setBlockNumber(blockNumber);
					
					/**Read block request call **/
											
						responseArray = dataStub.readBlock(readBlockReqObj.build().toByteArray());
						ReadBlockResponse readBlockResObj = ReadBlockResponse.parseFrom(responseArray);
						
						if(readBlockResObj.getStatus()==Constants.STATUS_FAILED)
						{
							System.out.println("In method openFileGet(), readError");
							System.exit(0);
						}
						
						responseArray = readBlockResObj.getData(0).toByteArray();						
						String str = new String(responseArray, StandardCharsets.UTF_8);						
						fileWriteObj.writeonly(str);												

				}
				
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NotBoundException e) {
			System.out.println("Exception caught: NotBoundException ");			
		} catch (RemoteException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		fileWriteObj.closeFile();
		
	}
	
	
	
	
	
}
