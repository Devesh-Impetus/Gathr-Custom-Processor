package com.yourcompany.util;

import com.yourcompany.component.ss.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FPAggregator implements MessageProcessor{
    private static final Logger log = LoggerFactory.getLogger(FPAggregator.class
            .getName());
    private final Map<String, Map<String, Map<String, String>>> rowKeyToColumnToColumnValMap = new HashMap<>();
    public static final String MESSAGE_SEPARATOR = "143";
    private final char messageSeparator;
    private final Map<String, String> colToColValMap = new HashMap<>();
   // private UniqueKeyGenerator uniqueKeyGenerator;
    public FPAggregator() {
        messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
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
}
