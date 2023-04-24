package com.yourcompany.component.ss.emitter.hbase;


import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.component.ss.emitter.util.UniqueKeyGenerator;
import com.yourcompany.util.SaltGenerator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class HBaseAAAInfoWriterNew implements Runnable {

    static Logger log = LoggerFactory.getLogger(HBaseAAAInfoWriterNew.class.getName());
    private Map<String, Map<Long, String>> aaaInfoTotimestampToeventInfo;
    //Read the partition Table
    private Set<PartitionTable> setOfPatitionTableObject;

    public HBaseAAAInfoWriterNew(Map<String, Map<Long, String>> aaaInfoTotimestampToeventInfo) {
        this.aaaInfoTotimestampToeventInfo = aaaInfoTotimestampToeventInfo;
        // setOfPatitionTableObject = PartitionManager.getInstance().getSetOfTableMap().get(Constants.PARTITION_DB_NAME);
    }

    public void run() {
        // TODO Auto-generated method stub
        try {
            insertAAAInfo();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            Writer writer = new StringWriter();
            PrintWriter printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            String s = writer.toString();
            log.error(s);
        }

    }

    private void insertAAAInfo() throws Exception {
        FileWriter writer = null;
        log.debug("Entered in a HBaseAAAInfoWriterNew.insertAAAInfo method ");
        UniqueKeyGenerator uniqueKeyGenerator = UniqueKeyGenerator.getInstance();
        Map<String, List<Put>> listofInsertion = new HashMap<String, List<Put>>();
        log.debug("Total unique AAA information interating for this batch" + aaaInfoTotimestampToeventInfo.size());
        Map<String, String> hbaseConfigProp = new HashMap<>();
        try (InputStream propInStr = Thread.currentThread().getContextClassLoader().getResourceAsStream(Constants.HBASE_CONFIG_FILE_NAME)) {
            Properties props = new Properties();
            props.load(propInStr);
            for (Map.Entry<Object, Object> propEntry : props.entrySet()) {
                hbaseConfigProp.put((String) propEntry.getKey(), (String) propEntry.getValue());
            }
        }

        for (String uniquekey : aaaInfoTotimestampToeventInfo.keySet()) {

            Map<Long, String> timestampToeventinfo = aaaInfoTotimestampToeventInfo.get(uniquekey);
            if (!(uniquekey == null || uniquekey.equalsIgnoreCase("0"))) {


                List<Put> listOfActivity = new ArrayList<Put>();

                StringBuilder rowKey = new StringBuilder();
                for (Long timestamp : timestampToeventinfo.keySet()) {

                    rowKey.setLength(0);
                    rowKey.append(SaltGenerator.getSalt(uniquekey, 12)).append("_").append(uniquekey).append("_");
                    LocalDateTime l = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC).toLocalDate().atStartOfDay();
                    rowKey.append(l.toEpochSecond(ZoneOffset.UTC));

                    /*String tmp = PartitionManager.getInstance().getTableName(setOfPatitionTableObject, timestamp);
                    if (tmp.startsWith("_")) {

                        rowKey.append(tmp.split("_")[1]);
                    } else {
                        rowKey.append(tmp);
                    }*/

                    Put objput = new Put(Bytes.toBytes(rowKey.toString().trim()));//rowkey


                    Map<Integer, String> appeventMap = new HashMap<Integer, String>();

                    boolean forThisBatchFPEvents = true;
                    boolean forThisBatchTwitterEvents = true;
                    boolean forThisBatchYouTubeEvents = true;
                    boolean forThisBatchInstagramEvents = true;

                    for (String eventname : timestampToeventinfo.get(timestamp).split(",")) {
                        //We need tablename like APPtype_EventName_N_starttime_enedtime

                        String[] evename = eventname.split("_");
                        int apptype = Integer.parseInt(evename[0]);
                        if (apptype == Integer.parseInt(Constants.FACEBOOK_ID) && forThisBatchFPEvents) {
                            forThisBatchFPEvents = false;
                        } else if (apptype == Integer.parseInt(Constants.TWITTER_ID) && forThisBatchTwitterEvents) {
                            forThisBatchTwitterEvents = false;

                        } else if (apptype == Integer.parseInt(Constants.YOUTUBE_ID) && forThisBatchYouTubeEvents) {
                            forThisBatchYouTubeEvents = false;

                        } else if (apptype == Integer.parseInt(Constants.INSTAGRAM_ID) && forThisBatchInstagramEvents) {
                            forThisBatchInstagramEvents = false;
                        }
                        String eventtype = evename[1];
                        if (appeventMap.containsKey(apptype)) {
                            String eventlist = appeventMap.get(apptype);
                            appeventMap.put(apptype, eventlist + "," + eventtype);
                        } else {
                            appeventMap.put(apptype, eventtype);
                        }

                    }

                    for (Integer apptype : appeventMap.keySet()) {
                        listOfActivity = new ArrayList<Put>();

                        String value = appeventMap.get(apptype).trim();//99,99,99
                        String family = String.valueOf(apptype).trim();//510
                        String column = uniqueKeyGenerator.getUniqueKey(String.valueOf(timestamp)); //1516865100_1000027
                        objput.addColumn(Bytes.toBytes(String.valueOf(apptype)),//Bytes.toBytes("test"),Bytes.toBytes("raju"));
                                Bytes.toBytes(String.valueOf(column)), Bytes.toBytes(value.trim()));

                        listOfActivity.add(objput);

                    }

                    //String tableName="N"+PartitionManager.getInstance().getTableName(setOfPatitionTableObject, timestamp);

                    String tableName = hbaseConfigProp.get(Constants.HBASE_FP_TABLE_AAA_TO_EVENT).trim();
                    //log.debug("Table Name Calculated as "+tableName);
                    if (!listofInsertion.containsKey(tableName)) {
                        listofInsertion.put(tableName, listOfActivity);

                    } else {
                        if (listOfActivity != null) {
                            List<Put> tempPutlist = listofInsertion.get(tableName);
                            tempPutlist.addAll(listOfActivity);

                            listofInsertion.get(tableName).addAll(listOfActivity);
                            //System.out.println(listofInsertion.get(tableName).size());
                        }
                    }

                }
            }

        }

        if (listofInsertion.size() > 0) {
            log.debug("start inserting the aaa information");
            HBaseAccessLayer objHbaseDataAccess = new HBaseAccessLayer();

            for (String tablename : listofInsertion.keySet()) {
                log.debug("AAA information Insertion started in table " + tablename + " with " + listofInsertion.get(tablename).size() + " records");
                int retrycount = Integer.parseInt(Constants.INSERTION_RETRY_COUNT);

                while (retrycount > 0) {
                    try {

                        retrycount = objHbaseDataAccess.InsertRecord(tablename, listofInsertion.get(tablename), retrycount);
                        if (retrycount == 0) {
                            HBaseMessageConsumer.flagIfexception = false;
                            log.debug("exception occured while inserting messages in batch at  " + this.getClass().getName());
                        }
                    } catch (IOException | InterruptedException e) {
                        Writer writerq = new StringWriter();
                        PrintWriter printWriter = new PrintWriter(writer);
                        e.printStackTrace(printWriter);
                        String s = writerq.toString();
                        log.error(s);

                    }

                }
                log.debug("finished AAA information insertion in table " + tablename + " for records size " + listofInsertion.get(tablename).size());
            }
        }

        log.debug("Exited from a HBaseAAAInfoWriterNew.insertAAAInfo method ");
    }
}
