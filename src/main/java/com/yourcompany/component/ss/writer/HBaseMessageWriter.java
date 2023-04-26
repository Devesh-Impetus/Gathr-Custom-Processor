package com.yourcompany.component.ss.writer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cleartrail.fingerprinting.globalparameters.GlobalStats;
import com.cleartrail.fingerprinting.insertionmodule.constants.Constants;
import com.cleartrail.fingerprinting.insertionmodule.hbase.layer.HBaseAccessLayer;
import com.cleartrail.fingerprinting.insertionmodule.reader.ConfigurationReader;
import com.cleartrail.fingerprinting.insertionmodule.uniquevalue.generator.UniqueKeyGnerator;
import com.cleartrail.fingerprinting.insertionmodule.utils.BucketGenrator;
import com.cleartrail.fingerprinting.insertionmodule.utils.SaltGenerator;

import com.yourcompany.component.ss.common.Constants;


public class HBaseMessageWriter implements Runnable {

	private static Logger log = LoggerFactory.getLogger(HBaseMessageWriter.class.getName());
	
	private Map<Integer, Map<String, String>> timestamoToipaddressTomobilenumber;
	private Map<String,String> propertyToValue;
	private int batchSize;
	private String eventName;//table name will be same as event name
	private char recordSeperator;
	private char messageSeperator;
	Map<String,String> GeneralpropertyToValue=null;
	ConfigurationReader configurationReader =null;
	private	int regionmod ;
	private	int startindex ;
	private	int endindex ;
	private	int retrycount;
	private long startTime = 0 ;
	private long endTime = 0 ;

	private int numberOfBuckets= Integer.parseInt(Constants.NO_OF_SALTS);
	private FileWriter fileWriter ;
	private boolean toDump;
	
	public HBaseMessageWriter(
			Map<Integer, Map<String, String>> timestamoToipaddressTomobilenumber,Map<String,String> propertyToValue,int batchSize,String eventName,char recordSeperator,char messageSeperator) {
		this.timestamoToipaddressTomobilenumber = timestamoToipaddressTomobilenumber;
		this.propertyToValue=propertyToValue;
		this.batchSize=batchSize;
		this.eventName=eventName;
		this.recordSeperator=recordSeperator;
		this.messageSeperator=messageSeperator;
		
		configurationReader=ConfigurationReader.getInstance();
		GeneralpropertyToValue=configurationReader.getGeneralpropertyToValue();
		
		regionmod =Integer.parseInt(GeneralpropertyToValue.get(Constants.REGION_MOD));
		//startindex =Integer.parseInt(GeneralpropertyToValue.get(Constants.START_INDEX));
		endindex =Integer.parseInt(GeneralpropertyToValue.get(Constants.END_INDEX));
		
		retrycount=Integer.parseInt(GeneralpropertyToValue.get(Constants.INSERTION_RETRY_COUNT));
		toDump=Boolean.parseBoolean(ConfigurationReader.getInstance().getGeneralpropertyToValue().get(Constants.IS_WRITE_DUMP_ENABLED));
	}

	
	public void run() {
		
		try {
			startTime = System.currentTimeMillis();
			insertMessages();
			endTime = System.currentTimeMillis();
			GlobalStats.TIME_TAKEN_TO_INSERT_RECORDS += (endTime - startTime);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			 StringWriter errors = new StringWriter();
				e.printStackTrace(new PrintWriter(errors));
				String error=e.toString();
				log.error(error);
		}
	}

	/*private void insertMessages() throws Exception  
	{
		log.info("Entered at  "+this.getClass().getName()+".insertMessages()");
		
		int appenderValue;
		int PreviousTimeStamp ;
		retrycount=3;
	
		List<Integer> listOfTimestamp=new ArrayList<Integer>();
		listOfTimestamp.addAll(timestamoToipaddressTomobilenumber.keySet());
		try {
			
			List<Put> listOfPut = new ArrayList<Put>();
			
			for(int i=0;i<listOfTimestamp.size() && HBaseMessageConsumer.flagIfexception;i++)
			{
				Map<String,String> ipaddressTomobilenumber=timestamoToipaddressTomobilenumber.get(listOfTimestamp.get(i));
				String recordValue="";
				StringBuilder recordValue1 = new StringBuilder();
				
				
				for (Map.Entry<String, String> entry : ipaddressTomobilenumber.entrySet())
				{
					if(entry.getKey()!=null)
					{
				     //recordValue=recordValue+entry.getKey()+messageSeperator;
				     
				     recordValue1.append(recordValue+entry.getKey()+messageSeperator);
					}
					if(entry.getValue()!=null)
					{
						//recordValue=recordValue+""+entry.getValue();
						recordValue1.append(""+entry.getValue());
					}
					if(!recordValue1.equals(""))
					{
						//recordValue=recordValue+recordSeperator;
						recordValue1.append(recordSeperator);
					}
				}
				
				int key = listOfTimestamp.get(i);

				//Get the index value for the given timestamp
				
			
				
				
				
				
				//Generate the key for a hbase.
				String skey = UniqueKeyGnerator.getInstance().getUniqueKey(String.valueOf(key));
				
				if(toDump){
                    String dumpLocation=ConfigurationReader.getInstance().getGeneralpropertyToValue().get(Constants.DUMP_LOCATION);
                    File file=new File(dumpLocation+"Event_DB_"+GlobalStats.FILE_START_TIME+"_"+GlobalStats.FILE_LASTSYNC_TIME);
                    //System.out.println();
                    if(!file.exists()){
                        file.createNewFile();
                        fileWriter=new FileWriter(file,true);
                        fileWriter.write("TimeStamp,Value"+"\n");
                    }
                    else{
                    	fileWriter=new FileWriter(file,true);
                    }
                if(fileWriter!=null){
                	//fileWriter.write(skey+","+recordValue);
                	fileWriter.write(skey+","+recordValue1.toString());
                	
                	fileWriter.write("\n");
                    if(fileWriter!=null)
                    	fileWriter.close();   
                }
                   
                   
                }
				
				//Put put=new Put(Bytes.toBytes(listOfTimestamp.get(i)));
				Put put=new Put(Bytes.toBytes(skey));
				
				//put.add(Bytes.toBytes(Constants.QUERY_TAB_FAMILY_NAME), Bytes.toBytes(Constants.QUERY_TAB_QUAL),Bytes.toBytes(recordValue));
				put.addColumn(Bytes.toBytes(Constants.QUERY_TAB_FAMILY_NAME), Bytes.toBytes(Constants.QUERY_TAB_QUAL),Bytes.toBytes(recordValue1.toString()));
				listOfPut.add(put);
				
		
				
				if ((listOfPut.size() == batchSize || (i + 1) == listOfTimestamp.size()) ) 
				{
					log.info("inserting "+listOfPut.size()+" records in "+eventName +" tables");
							
					if(HBaseMessageConsumer.flagIfexception)
					{
						while (retrycount>0)
						{
							{
								HBaseAccessLayer objHbaseDataAccess = new HBaseAccessLayer();  
								
								//objHbaseDataAccess.InsertRecord("N"+activityTableName, listOfActivity,retrycount);
								//
							retrycount=	objHbaseDataAccess.InsertRecord(eventName, listOfPut,retrycount);
							
							if(retrycount==0)
							{
								HBaseMessageConsumer.flagIfexception=false;
								log.info("failed to insert the record total "+listOfPut.size()+" records in "+eventName +" table");
								log.info("exception occured while inserting messages in batch at  "+this.getClass().getName());
							}
							else if(retrycount==-1)
							{
								UniqueKeyGnerator.getInstance().writeValuetoZookeeperMap();
								HBaseMessageConsumer.flagIfexception=true;
							}
								
								
							}
						}
					}
					
					listOfPut.clear();
				}
			}
		}catch (UnknownHostException e) 
		{
			StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error=e.toString();
            log.error(error);
			log.error("exception occured while inserting messages at  "+this.getClass().getName()+" exception details: "+e.getMessage());
			HBaseMessageConsumer.flagIfexception=false;
			
		} 
		catch (IOException e) {
			StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error=e.toString();
            log.error(error);
			log.error("exception occured while inserting messages at  "+this.getClass().getName()+" exception details: "+e.getMessage());
			HBaseMessageConsumer.flagIfexception=false;
			
		}
		log.info("Exited from  "+this.getClass().getName()+".insertMessages()");
	
	}
*
*/
	
	
	
	private void insertMessages() throws Exception  
	{
		log.info("Entered at  "+this.getClass().getName()+".insertMessages()");
		
		int appenderValue;
		int PreviousTimeStamp ;
		//retrycount=3;
	
		List<Integer> listOfTimestamp=new ArrayList<Integer>();
		listOfTimestamp.addAll(timestamoToipaddressTomobilenumber.keySet());
		try {
			
			List<Put> listOfPut = new ArrayList<Put>();
			
			for(int i=0;i<listOfTimestamp.size() ;i++)
			{
				Map<String,String> ipaddressTomobilenumber=timestamoToipaddressTomobilenumber.get(listOfTimestamp.get(i));
				String recordValue="";
				StringBuilder recordValue1 = new StringBuilder();
				
				
				for (Map.Entry<String, String> entry : ipaddressTomobilenumber.entrySet())
				{
					if(entry.getKey()!=null)
					{
				     //recordValue=recordValue+entry.getKey()+messageSeperator;
				     
				     recordValue1.append(recordValue+entry.getKey()+messageSeperator);
					}
					if(entry.getValue()!=null)
					{
						//recordValue=recordValue+""+entry.getValue();
						recordValue1.append(""+entry.getValue());
					}
					if(!recordValue1.equals(""))
					{
						//recordValue=recordValue+recordSeperator;
						recordValue1.append(recordSeperator);
					}
				}
				
				int key = listOfTimestamp.get(i);

				//Get the index value for the given timestamp			
				
				
				//Generate the key for a hbase.
				String skey = UniqueKeyGnerator.getInstance().getUniqueKey(String.valueOf(key));
				
				if(toDump){
                    String dumpLocation=ConfigurationReader.getInstance().getGeneralpropertyToValue().get(Constants.DUMP_LOCATION);
                    File file=new File(dumpLocation+"Event_DB_"+GlobalStats.FILE_START_TIME+"_"+GlobalStats.FILE_LASTSYNC_TIME);
                    //System.out.println();
                    if(!file.exists()){
                        file.createNewFile();
                        fileWriter=new FileWriter(file,true);
                        fileWriter.write("TimeStamp,Value"+"\n");
                    }
                    else{
                    	fileWriter=new FileWriter(file,true);
                    }
                if(fileWriter!=null){
                	//fileWriter.write(skey+","+recordValue);
                	fileWriter.write(skey+","+recordValue1.toString());
                	
                	fileWriter.write("\n");
                    if(fileWriter!=null)
                    	fileWriter.close();   
                }
                   
                   
                }
				
				
				StringBuilder sb = new StringBuilder();
				String arr[] = eventName.split("_");// eventId_Day Start Time_EndTime 199_1516838400_1516924800

				// skey taking the first element of skey which is event timestamp, and
				// generating the salt on event time.
				// Event time rounded off from Maven it self but in some cases it's coming in seconds
				// here we are rounding off the time for minutes.
				
				String eventTimeRoundedOff=String.valueOf(BucketGenrator.roundOff(Long.valueOf(skey.split("_")[0].trim()))).trim();				
				
				sb.append(SaltGenerator.getSalt(eventTimeRoundedOff,numberOfBuckets))
						.append("_").append(arr[0]).append("_").append(arr[1]).append("_").append(eventTimeRoundedOff);

				Put put = new Put(Bytes.toBytes(sb.toString()));
				put.addColumn(Bytes.toBytes(Constants.QUERY_TAB_FAMILY_NAME),
						Bytes.toBytes(Constants.QUERY_TAB_QUAL + skey.split("_")[1]),
						Bytes.toBytes(recordValue1.toString()));
				log.debug("Rowkey :: "+sb);
				listOfPut.add(put);
				
		
				
				if ((listOfPut.size() == batchSize || (i + 1) == listOfTimestamp.size()) ) 
				{
					log.info("inserting "+listOfPut.size()+" records for event : "+eventName.split("_")[0] );
							
					//if(HBaseMessageConsumer.flagIfexception)
					{
						while (retrycount>0)
						{
							{
								HBaseAccessLayer objHbaseDataAccess = new HBaseAccessLayer();  
								
								//objHbaseDataAccess.InsertRecord("N"+activityTableName, listOfActivity,retrycount);
								//
							       //retrycount=	objHbaseDataAccess.InsertRecord(eventName, listOfPut,retrycount);
								
								//retrycount=	objHbaseDataAccess.InsertRecord("Finger_Printing-Event_to_AAA_Option6", listOfPut,retrycount);
								retrycount=	objHbaseDataAccess.InsertRecord(ConfigurationReader.getInstance().getGeneralpropertyToValue().get(Constants.HBASE_FP_TABLE_EVENT_TO_AAA).trim(), listOfPut,retrycount);
								
							
							if(retrycount==0)
							{
								HBaseMessageConsumer.flagIfexception=false;
								log.error("failed to insert the record total "+listOfPut.size()+" records in "+eventName +" table");
								log.error("exception occured while inserting messages in batch at  "+this.getClass().getName());
							}
							else if(retrycount==-1)
							{
								UniqueKeyGnerator.getInstance().writeValuetoZookeeperMap();
								HBaseMessageConsumer.flagIfexception=true;
							}
								
								
							}
						}
					}
					
					listOfPut.clear();
				}
			}
		}catch (UnknownHostException e) 
		{
			StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error=e.toString();
            log.error(error);
			log.error("exception occured while inserting messages at  "+this.getClass().getName()+" exception details: "+e.getMessage());
			HBaseMessageConsumer.flagIfexception=false;
			
		} 
		catch (IOException e) {
			StringWriter errors = new StringWriter();
            e.printStackTrace(new PrintWriter(errors));
            String error=e.toString();
            log.error(error);
			log.error("exception occured while inserting messages at  "+this.getClass().getName()+" exception details: "+e.getMessage());
			HBaseMessageConsumer.flagIfexception=false;
			
		}
		log.info("Exited from  "+this.getClass().getName()+".insertMessages()");
	
	}

}
