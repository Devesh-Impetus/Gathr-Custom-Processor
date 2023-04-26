package com.yourcompany.util;

import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.component.ss.writer.HBaseMessageWriter;
import com.yourcompany.custom.ss.partitioner.PartitionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FPAggregator implements MessageProcessor{
    private static final Logger log = LoggerFactory.getLogger(FPAggregator.class
            .getName());
    private final Map<String, Map<String, Map<String, String>>> rowKeyToColumnToColumnValMap = new HashMap<>();
    public static final String MESSAGE_SEPARATOR = "143";
    public static final String RECORD_SEPARATOR = "140";
    public static final String BATCH_SIZE = "50000";
    private final char messageSeparator;
    private final char recordSeparator;
    private final char batchSize;
    private final Map<String, String> colToColValMap = new HashMap<>();
   // private UniqueKeyGenerator uniqueKeyGenerator;
    public FPAggregator() {
        messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
        recordSeparator = (char) Integer.parseInt(RECORD_SEPARATOR);
        batchSize = (char) Integer.parseInt(BATCH_SIZE);
       // uniqueKeyGenerator = UniqueKeyGenerator.getInstance();
    }
    @Override
    public Map<String, String> processMessages(List<String> listOfMessageString) {
        log.info("Inside" + this.getClass().getName() +".processMessages()");

        Map<String, Map<Long, String>> aaatotimestamptosetofevent = new HashMap<>();
        UniqueKeyGenerator uniqueKeyGenerator=UniqueKeyGenerator.getInstance();
        // aaa to ip to value
        for (String message : listOfMessageString) {

            String[] tokens = message.split(String.valueOf(messageSeparator));
            Map<String, String> propertyTovalue = new HashMap<>();
            for (int i = 0; i < tokens.length - 1;) {
                propertyTovalue.put(tokens[i++], tokens[i++]);
            }

            String keymobile = propertyTovalue.get(Constants.MOBILE_CODE);
            String keyIP = propertyTovalue.get(Constants.IPADDDRESS_CODE);
            String serverIP = propertyTovalue.get(Constants.SERVER_IP_CODE);
            String IMSI  = propertyTovalue.get(Constants.IMSI_CODE);
            String IMEI = propertyTovalue.get(Constants.IMEI_CODE);
            String Tx = propertyTovalue.get(Constants.TX_CODE);
            String mavId  = propertyTovalue.get(Constants.MAVEN_ID_CODE);
            String ifId = propertyTovalue.get(Constants.IF_ID_CODE);

            String app = propertyTovalue.get(Constants.FACEBOOK_ID);
            if (app == null)
            {
                app = propertyTovalue.get(Constants.TWITTER_ID);
            }
            if (app == null)
            {
                app = propertyTovalue.get(Constants.YOUTUBE_ID);
            }
            if (app == null)
            {
                app = propertyTovalue.get(Constants.INSTAGRAM_ID);
            }
            if (app == null)
            {
                app = propertyTovalue.get(Constants.SNAPCHAT_ID);
            }
            if (app == null)
            {
                app = propertyTovalue.get(Constants.TELEGRAM_ID);
            }
            if (app == null)
            {
                app = propertyTovalue.get(Constants.TIKTOK_ID);
            }

            String activity = propertyTovalue.get(Constants.USER_EVENT_CODE);
            String qualifierValue = app+ "_" + activity+ messageSeparator;

            qualifierValue = IMSI != null ? (qualifierValue + IMSI + messageSeparator) : (qualifierValue + messageSeparator);
            qualifierValue = IMEI!=null ? (qualifierValue + IMEI + messageSeparator) : (qualifierValue + messageSeparator);
            qualifierValue = Tx!=null ? qualifierValue + Tx + messageSeparator : (qualifierValue + messageSeparator);
            qualifierValue = keyIP!=null ? qualifierValue + keyIP + messageSeparator : (qualifierValue + messageSeparator);
           // qualifierValue = clientIp!=null ? qualifierValue + clientIp + messageSeparator : (qualifierValue + messageSeparator);
            qualifierValue = serverIP!=null ? qualifierValue + serverIP + messageSeparator : (qualifierValue + messageSeparator);
            qualifierValue = ifId!=null ? qualifierValue + ifId + messageSeparator : qualifierValue + messageSeparator;
            qualifierValue = mavId!=null ? qualifierValue + mavId + messageSeparator : qualifierValue + messageSeparator;

            String time = propertyTovalue.get(Constants.TIME_EVENT_CODE);

            if (keymobile != null) {
                if(aaatotimestamptosetofevent.containsKey(keymobile)){
                    //ntwk already present
                    Map<Long, String> timeToSetOfValue=	aaatotimestamptosetofevent.get(keymobile);
                    if(timeToSetOfValue.containsKey(Long.parseLong(time))){
                        //timestamp for ntwk exists
                        String valString=timeToSetOfValue.get(Long.parseLong(time));
                        valString=valString+","+qualifierValue;
                        timeToSetOfValue.put(Long.parseLong(time), valString);

                    }else{
                        // from this ip activity performed at new timestamp
                        timeToSetOfValue.put(Long.parseLong(time), qualifierValue);
                    }
                }

                else{
                    //a new network
                    Map<Long, String> timeToSetOfValue=new HashMap<>();
                    timeToSetOfValue.put(Long.parseLong(time), qualifierValue);
                    aaatotimestamptosetofevent.put(keymobile,timeToSetOfValue);


                }
            }
            if (keyIP != null) {
                if(aaatotimestamptosetofevent.containsKey(keyIP)){
                    //ntwk already present
                    Map<Long, String> timeToSetOfValue=	aaatotimestamptosetofevent.get(keyIP);
                    if(timeToSetOfValue.containsKey(Long.parseLong(time))){
                        //timestamp for ntwk exists
                        String valString=timeToSetOfValue.get(Long.parseLong(time));
                        valString=valString+","+qualifierValue;
                        timeToSetOfValue.put(Long.parseLong(time), valString);

                    }else{
                        // from this ip activity performed at new timestamp
                        timeToSetOfValue.put(Long.parseLong(time), qualifierValue);
                    }
                }

                else{
                    //a new network
                    Map<Long, String> timeToSetOfValue=new HashMap<>();
                    timeToSetOfValue.put(Long.parseLong(time), qualifierValue);
                    aaatotimestamptosetofevent.put(keyIP,timeToSetOfValue);
                }
            }
        }


        for(String uniquekey : aaatotimestamptosetofevent.keySet())
        {

            Map<Long, String> timestampToeventinfo =aaatotimestamptosetofevent.get(uniquekey);
            if(!(uniquekey==null || uniquekey.equalsIgnoreCase("0"))) {

                StringBuilder rowKey=new StringBuilder();
                Map<String, Map<String, String>> appTypeTocolToColValMap = new HashMap<>();
                for(Long timestamp : timestampToeventinfo.keySet())
                {
                    rowKey.setLength(0);
                    rowKey.append(SaltGenerator.getSalt(uniquekey, 12)).append("_").append(uniquekey).append("_");
                    LocalDateTime l= LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).toLocalDate().atStartOfDay();
                    rowKey.append(l.toEpochSecond(ZoneOffset.UTC));

                    Map<Integer,String> appeventMap= new HashMap<Integer, String>();
                    for(String eventname : timestampToeventinfo.get(timestamp).split(","))
                    {
                        //We need tablename like APPtype_EventName_N_starttime_enedtime

                        String [] evename =eventname.split("_");
                        int apptype=Integer.parseInt(evename[0]);
                        String eventtype=evename[1];
                        if(appeventMap.containsKey(apptype))
                        {
                            String eventlist = appeventMap.get(apptype);
                            appeventMap.put(apptype,eventlist+","+eventtype);
                        }
                        else
                        {
                            appeventMap.put(apptype,eventtype);
                        }

                    }

                    for(Integer apptype : appeventMap.keySet())
                    {
                        Map<String, String> colToColValMap = new HashMap<>();
                        String value=appeventMap.get(apptype).trim();//99,99,99
                        String family =String.valueOf(apptype).trim();//510
                        String column = uniqueKeyGenerator.getUniqueKey(String.valueOf(timestamp));//1516865100_1000027
                        colToColValMap.put(column, value);
                        appTypeTocolToColValMap.put(family, colToColValMap);
                    }
                    rowKeyToColumnToColumnValMap.put(String.valueOf(rowKey), appTypeTocolToColValMap);
                }
            }

        }
        log.info("mappings:" + rowKeyToColumnToColumnValMap);
        return colToColValMap;
    }
    
	@Override
	public Map<String, String> processMessagesForFPIngress(List<String> listOfMessageString) {

		log.info("Entered at  "+this.getClass().getName()+".processMessages()");
		
		Map<String,Map<Integer,Map<String,String>>> eventTotimestampToipaddressTomobilenumber=new LinkedHashMap<String,Map<Integer,Map<String,String>>>();
		
		Map<String,Map<Integer,Map<String,String>>> eventtableTotimestampToipaddressTomobilenumber=new LinkedHashMap<String,Map<Integer,Map<String,String>>>();
		
		// get the range set in configuration
    	int range=1;
    	int realTimespan =0;
    	
    	
//    	Map<String,String> configPropertyToValue=ConfigurationReader.getInstance().getGeneralpropertyToValue();
//    	//Read the partition Table
//    	Set<PartitionTable> setOfPatitionTableObject =PartitionManager.getInstance().getSetOfTableMap().get(Constants.PARTITION_DB_NAME);
    	
//    	char messageseperator=(char)Integer.parseInt(configPropertyToValue.get(Constants.MESSAGE_SEPERATOR));
    	
//    	if(configPropertyToValue.get(Constants.TIME_RANGE)!=null)
//    	range=Integer.parseInt(configPropertyToValue.get(Constants.TIME_RANGE));
    	
		for(String message:listOfMessageString){
			
			Map<Integer,Set<String>> timestampToEventInfo=null;
			Set<String> setofEvents=null;
			
			//get the general configuration map to know the time range from configuration
							
			String[] tokens = message.split(MESSAGE_SEPARATOR);
		    Map<String, String> propertyTovalue = new HashMap<String,String>();
		    for (int i=0; i<tokens.length-1; ){ 
		    	propertyTovalue.put(tokens[i++], tokens[i++]);
		    }
		    Map<String,String> ipaddressTomobilenumber;
		    Map<Integer,Map<String,String>> timestampToipaddressTomobilenumber;
		    
		    //1100,175478633,3006,99,3003,1403523840,59,59
		    /*facebookcode,59
		    twittercode,60
		    ipaddresscode,1100
		    mobilecode,3008	
		    ueventcode,3006
		    timestampcode,3003*/
			//get the timestamp in property to value map convert it into range supplied
		    Integer timestamp=0;
	    	if(propertyTovalue.get(propertyTovalue.get(Constants.TIME_EVENT_CODE))!=null)
	    		{
		     timestamp=Integer.parseInt(propertyTovalue.get(Constants.TIME_EVENT_CODE));
		     realTimespan=timestamp;
		  
//		     GlobalStats.LastFrameTime  = timestamp;
	    		}
	    	
	    	
	    	timestamp=(timestamp/range)*range;
		    
		    String ipaddress=null;
		    if(propertyTovalue.get(Constants.IPADDDRESS_CODE)!=null)
		    ipaddress=(propertyTovalue.get(Constants.IPADDDRESS_CODE));
		    
		    String mobileNumber=null;
		    if(propertyTovalue.get(Constants.MOBILE_CODE)!=null) {
		    mobileNumber = (propertyTovalue.get(Constants.MOBILE_CODE));
		    }  
		    
		    String IMSICode = null;
		    if(mobileNumber != null) {
		    if(propertyTovalue.get(Constants.IMSI_CODE)!=null) {
		    	IMSICode = (propertyTovalue.get(Constants.IMSI_CODE));
				mobileNumber=  mobileNumber.concat(MESSAGE_SEPARATOR).concat(IMSICode);
			}else {				
				mobileNumber =  mobileNumber.concat(MESSAGE_SEPARATOR);
			}
		    } 
		    
		    String IMEICode = null;
		    if(mobileNumber != null) {
		    if(propertyTovalue.get(Constants.IMEI_CODE)!=null) {
		    	IMEICode = (propertyTovalue.get(Constants.IMEI_CODE));
		    	mobileNumber =  mobileNumber.concat(MESSAGE_SEPARATOR).concat(IMEICode);				 	
		    }else {
		    	mobileNumber =	mobileNumber.concat(MESSAGE_SEPARATOR);			
			}
		    }  
		    
		    String eventName="";
		    if(propertyTovalue.get(Constants.USER_EVENT_CODE)!=null){
		    	eventName=propertyTovalue.get(Constants.USER_EVENT_CODE);
		    }
				    String app = propertyTovalue.get(Constants.FACEBOOK_ID);
		        
			if (app == null) 
			{
				app = propertyTovalue.get(Constants.TWITTER_ID);
			}
			if (app == null) 
			{
				app = propertyTovalue.get(Constants.YOUTUBE_ID);
			}
			if (app == null) 
			{
				app = propertyTovalue.get(Constants.INSTAGRAM_ID);
			}
			if (app == null)
			{
				app = propertyTovalue.get(Constants.TIKTOK_ID);
			}
			if (app == null)
			{
				app = propertyTovalue.get(Constants.TELEGRAM_ID);
			}
			if (app == null)
			{
				app = propertyTovalue.get(Constants.SNAPCHAT_ID);
			}
			
//			if(app.equals(facebookid+""))
//			{
//				GlobalStats.INSERTED_FBEvents++;
//			}
//			else if(app.equals(twitterid+""))
//			{
//				GlobalStats.INSERTED_TWitterEvents++;
//			}
//			else if(app.equals(youtubeid+""))
//			{
//				GlobalStats.INSERTED_YouTubeEvents++;
//			}
//			else if(app.equals(instagramId+""))
//			{
//				GlobalStats.INSERTED_InstagramEvents++;
//			}
//			else if(app.equals(tiktokid+"")) {
//				GlobalStats.INSERTED_TiktokEvents++;
//			}
//			else if(app.equals(telegramid+"")) {
//				GlobalStats.INSERTED_TelegramEvents++;
//			}
//			else if(app.equals(snapchatid+"")) {
//				GlobalStats.INSERTED_SnapchatEvents++;
//			}
		    //Get the event table name.
		    String eventTableName= eventName;
		    		//+PartitionManager.getInstance().getTableName(setOfPatitionTableObject, timestamp);
		    //get map based on the event name in property to value map
		    timestampToipaddressTomobilenumber=eventTotimestampToipaddressTomobilenumber.get(eventTableName);
		    if(timestampToipaddressTomobilenumber!=null && timestampToipaddressTomobilenumber.size()>0){
		    
		    	
		    	 ipaddressTomobilenumber=timestampToipaddressTomobilenumber.get(timestamp);
		    	
		    	if(ipaddressTomobilenumber!=null && ipaddressTomobilenumber.size()>0){
		    		String mobilenumb=ipaddressTomobilenumber.get(ipaddress);
		    		if(mobilenumb!=null){
		    			
		    		}
		    		else{
		    			
		    			ipaddressTomobilenumber.put(ipaddress,mobileNumber);
		    			
		    		}
		    	}
		    	else{
		    		// for the timestamp no record exists insert one 
		    		 ipaddressTomobilenumber=new HashMap<String, String>();
		    		 ipaddressTomobilenumber.put(ipaddress,mobileNumber);
		    		timestampToipaddressTomobilenumber.put(timestamp,ipaddressTomobilenumber);
		    		
		    	}
		    	
		    }
		    else{
		    	// no record from the supplied event name exists add one 
		    	timestampToipaddressTomobilenumber=new HashMap<Integer, Map<String,String>>();
		    	ipaddressTomobilenumber=new HashMap<String, String>();
		    	ipaddressTomobilenumber.put(ipaddress,mobileNumber);
		    	timestampToipaddressTomobilenumber.put(timestamp,ipaddressTomobilenumber);
		    	eventTotimestampToipaddressTomobilenumber.put(eventTableName,timestampToipaddressTomobilenumber);
		    }
		    
		}
		
		//Now Added tablename.
		Map<Integer,Map<String,String>> timestampToipaddressTomobilenumber;
		for(String eventname:eventTotimestampToipaddressTomobilenumber.keySet())
	    {
	    	timestampToipaddressTomobilenumber=eventTotimestampToipaddressTomobilenumber.get(eventname);
	    	
	    	for(int eventTimestamp : timestampToipaddressTomobilenumber.keySet())
	    	{	//String [] eveName=eventname.split("_");
	    		String eventTablename= eventname +PartitionManager.getTableName(eventTimestamp);
	    		
	    		//add value in existing key. 
	    		Map<Integer,Map<String,String>> tmpMap=eventtableTotimestampToipaddressTomobilenumber.get(eventTablename);
	    		if(tmpMap!=null)
	    		{
	    			tmpMap.put(eventTimestamp,timestampToipaddressTomobilenumber.get(eventTimestamp));
	    		}
	    		else
	    		{
	    			//Add new key
	    			Map<Integer,Map<String,String>> newMap = new HashMap <Integer,Map<String,String>> ();
                    newMap.put(eventTimestamp, timestampToipaddressTomobilenumber.get(eventTimestamp));
	    			eventtableTotimestampToipaddressTomobilenumber.put(eventTablename, newMap);
	    		}
	    	}
	    }
		
		for(String eventname:eventtableTotimestampToipaddressTomobilenumber.keySet())
		{
			new HBaseMessageWriter(eventtableTotimestampToipaddressTomobilenumber
					.get(eventname), batchSize, eventname, recordSeparator, messageSeparator).run();
		}
		
		
		//log.info("Exited from  "+this.getClass().getName()+".processMessages()");
//		snmpHandler.checkAndWriteIntoFile(batch);
		return HBaseMessageConsumer.consumeMessages(eventtableTotimestampToipaddressTomobilenumber);
		
	
	}
}
