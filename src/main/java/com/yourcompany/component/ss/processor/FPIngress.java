package com.yourcompany.component.ss.processor;

import com.streamanalytix.framework.api.spark.processor.CustomProcessor;
import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.util.FPAggregator;
import com.yourcompany.util.MessageProcessor;
import com.yourcompany.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class FPIngress implements CustomProcessor {
    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611540615487274554L;

    /**
     * The Constant LOGGER.
     */
    private static final Log LOGGER = LogFactory.getLog(FPIngress.class);
    public static final String MESSAGE_SEPARATOR = "143";
    private final char messageSeparator;

    public FPIngress() {
        messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.error("inside init of FPIngress");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
        LOGGER.error("inside init of FPIngress host" + host);

    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataset) throws Exception {
        Map<String, String> columnToColumnValMap = getMessages(dataset.collectAsList());
        StructType structType = new StructType();
        SparkSession sparkSession = dataset.sparkSession();
        List<Row> nums = new ArrayList<>();
        List<String> values = new ArrayList<>();

        for (Map.Entry<String, String> map : columnToColumnValMap.entrySet()) {
            structType = structType.add(map.getKey(), DataTypes.StringType, false);
            values.add(map.getValue());
        }

        nums.add(RowFactory.create(values.toArray()));
        return sparkSession.createDataFrame(nums, structType);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
/*    @Override
    public List<Row> process(List<Row> rows) throws Exception {
        LOGGER.error("inside process of FPIngress");
        getMessages(rows);
        List<Row> finalList = new ArrayList<Row>();
        List<Row> nums = new ArrayList<>();
        List<String> listOfMessages = new ArrayList<>();

        for (Row row : rows) {
            //Map<Integer, String> objFPData = new HashMap<>();
            List<Object> values = new ArrayList<>();
            StructType structType = new StructType();
            String[] columns = row.schema().fieldNames();
            Arrays.sort(columns);
            StringBuilder hbaseColumn = new StringBuilder();
            StringBuilder hbaseColumnVal = new StringBuilder();
            String appId = Arrays.binarySearch(columns, "APPLICATION") >=0 ? row.getAs("APPLICATION").toString() : "";
            String activityId = Arrays.binarySearch(columns, "EVENTTYPE") >=0 ? row.getAs("EVENTTYPE").toString() : "";
            String imsi = Arrays.binarySearch(columns, "IMSI") >=0 ? row.getAs("IMSI").toString() : "";
            String imei = Arrays.binarySearch(columns, "IMEI") >=0 ? row.getAs("IMEI").toString() : "";
            String txType = Arrays.binarySearch(columns, "DATATX") >=0 ? row.getAs("DATATX").toString() : "";
            String clientIp = Arrays.binarySearch(columns, "CLIENTIP") >=0 ? row.getAs("CLIENTIP") : "";
            String serverIp = Arrays.binarySearch(columns, "SERVERIP") >=0 ? row.getAs("SERVERIP") : "";
            String ifId = Arrays.binarySearch(columns, "IFID") >=0 ? row.getAs("IFID").toString() : "";
            String mavenId = Arrays.binarySearch(columns, "MAVENID") >=0 ? row.getAs("MAVENID").toString() : "";

            *//*if (Util.isNotNullOrEmpty(appId)) {
                hbaseColumnVal.append(activityId).append("_");
            }*//*

            if (Util.isNotNullOrEmpty(activityId)) {
                hbaseColumnVal.append(activityId).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(imsi)) {
                hbaseColumnVal.append(imsi).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(imei)) {
                hbaseColumnVal.append(imei).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(txType)) {
                hbaseColumnVal.append(txType).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(clientIp)) {
                if(clientIp.contains(":")) {
                    hbaseColumnVal.append(clientIp).append(messageSeparator);
                } else {
                    hbaseColumnVal.append(Util.ipToLong(clientIp)).append(messageSeparator);
                }
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(serverIp)) {
                if(clientIp.contains(":")) {
                    hbaseColumnVal.append(clientIp).append(messageSeparator);
                } else {
                    hbaseColumnVal.append(Util.ipToLong(serverIp)).append(messageSeparator);
                }
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(ifId)) {
                hbaseColumnVal.append(ifId).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            if (Util.isNotNullOrEmpty(mavenId)) {
                hbaseColumnVal.append(mavenId).append(messageSeparator);
            } else {
                hbaseColumnVal.append(messageSeparator);
            }

            hbaseColumnVal = new StringBuilder(hbaseColumnVal.substring(0, hbaseColumnVal.length() - 1));
            nums.add(RowFactory.create(hbaseColumnVal.toString()));

            hbaseColumn.append(System.currentTimeMillis() / 1000)
                    .append("_")
                    .append(Util.getUniqueNumber());

            *//*List<Object> values = new ArrayList<Object>();
     *//**//*String[] fields = row.schema().fieldNames();
            for (int i = 0; i < fields.length; i++) {*//**//*
                values.add("abc");
                values.add("_");
                values.add("def");*//*
            //  }
            values.add(hbaseColumnVal.toString());
            listOfMessages.add(hbaseColumnVal.toString());
            // structType = structType.add(hbaseColumn.toString(), DataTypes.StringType, false);
            GenericRowWithSchema rowWithSchema = new GenericRowWithSchema(values.toArray(), structType);
            GenericRow finalRow = new GenericRow(values.toArray());
            //  finalRow.schema().add("fingerprinting_data", DataTypes.StringType, Boolean.TRUE, Metadata.empty());
            finalList.add(finalRow);
        }
        LOGGER.error("exit process of SampleCustomRowListProcessor version1");
        return finalList;
    }*/

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {

    }

    public Map<String, String> getMessages(List<Row> rows) {
        List<String> messages = new ArrayList<>();
        // skipped 0th index row for header in the feed
        for (int i = 0; i < rows.size(); i++) {
            String[] columns = rows.get(i).schema().fieldNames();
            Arrays.sort(columns);
            StringBuilder message = new StringBuilder();
            String appId = Arrays.binarySearch(columns, "APPLICATION") >= 0 ? rows.get(i).getAs("APPLICATION").toString() : "";
            String activityId = Arrays.binarySearch(columns, "EVENTTYPE") >= 0 ? rows.get(i).getAs("EVENTTYPE").toString() : "";
            String mobile = Arrays.binarySearch(columns, "MOBILE") >= 0 ? rows.get(i).getAs("MOBILE").toString() : "";
            String imsi = Arrays.binarySearch(columns, "IMSI") >= 0 ? rows.get(i).getAs("IMSI").toString() : "";
            String imei = Arrays.binarySearch(columns, "IMEI") >= 0 ? rows.get(i).getAs("IMEI").toString() : "";
            String txType = Arrays.binarySearch(columns, "DATATX") >= 0 ? rows.get(i).getAs("DATATX").toString() : "";
            String clientIp = Arrays.binarySearch(columns, "CLIENTIP") >= 0 ? rows.get(i).getAs("CLIENTIP") : "";
            String serverIp = Arrays.binarySearch(columns, "SERVERIP") >= 0 ? rows.get(i).getAs("SERVERIP") : "";
            String ifId = Arrays.binarySearch(columns, "IFID") >= 0 ? rows.get(i).getAs("IFID").toString() : "";
            String mavenId = Arrays.binarySearch(columns, "MAVENID") >= 0 ? rows.get(i).getAs("MAVENID").toString() : "";
            String transactionEndTime = Arrays.binarySearch(columns, "TRANSACTIONENDTIME") >= 0 ? rows.get(i).getAs("TRANSACTIONENDTIME").toString() : "";

            if (Util.isNotNullOrEmpty(appId)) {
                message.append(appId).append(messageSeparator).append(appId).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(activityId)) {
                message.append(Constants.USER_EVENT_CODE).append(messageSeparator).append(activityId).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(clientIp)) {
                message.append(Constants.IPADDDRESS_CODE).append(messageSeparator).append(Util.ipToLong(clientIp)).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(mobile)) {
                message.append(Constants.MOBILE_CODE).append(messageSeparator).append(mobile).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(serverIp)) {
                message.append(Constants.SERVER_IP_CODE).append(messageSeparator).append(Util.ipToLong(serverIp))
                        .append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(imsi)) {
                message.append(Constants.IMSI_CODE).append(messageSeparator).append(imsi).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(imei)) {
                message.append(Constants.IMEI_CODE).append(messageSeparator).append(imei).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(txType)) {
                message.append(Constants.TX_CODE).append(messageSeparator).append(txType).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(mavenId)) {
                message.append(Constants.MAVEN_ID_CODE).append(messageSeparator).append(mavenId).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(ifId)) {
                message.append(Constants.IF_ID_CODE).append(messageSeparator).append(ifId).append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(transactionEndTime)) {
                message.append(Constants.TIME_EVENT_CODE).append(messageSeparator).append(transactionEndTime).append(messageSeparator);
            }
            messages.add(message.substring(0, message.length() - 1));
        }

        MessageProcessor messageProcessor = new FPAggregator();
        return messageProcessor.processMessages(messages);
    }
}
