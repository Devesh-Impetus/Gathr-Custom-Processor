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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FPAAAIngress implements CustomProcessor {
    public static final String MESSAGE_SEPARATOR = "143";
    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611540615487274554L;
    /**
     * The Constant LOGGER.
     */
    private static final Log LOGGER = LogFactory.getLog(FPAAAIngress.class);
    private final char messageSeparator;

    public FPAAAIngress() {
        messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.error("inside init of FPAAAIngress");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
        LOGGER.error("inside init of FPAAAIngress host" + host);

    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataset) throws Exception {
        getMessages(dataset.collectAsList());
        /*StructType structType = new StructType();
        SparkSession sparkSession = dataset.sparkSession();
        List<Row> nums = new ArrayList<>();
        List<String> values = new ArrayList<>();

        for (String rowKey : rowKeyToAppTocolumnToColumnValMap.keySet()) {
            Map<String, Map<String, String>> appToColToColValMap = rowKeyToAppTocolumnToColumnValMap.get(rowKey);

            for (String app : appToColToColValMap.keySet()) {
                Map<String, String> colToColValues = appToColToColValMap.get(app);
                for (Map.Entry<String, String> colToColVal : colToColValues.entrySet()) {
                    LOGGER.info("Row key:" + rowKey + ", App Id:" + app + ", column:" + colToColVal.getKey() +
                            ", value:" + colToColVal.getValue());
                    structType = structType.add(colToColVal.getKey(), DataTypes.StringType, false);
                    values.add(colToColVal.getValue());
                }
            }
        }

        *//*for (Map.Entry<String, String> map : columnToColumnValMap.entrySet()) {
            structType = structType.add(map.getKey(), DataTypes.StringType, false);
            values.add(map.getValue());
        }*//*

        nums.add(RowFactory.create(values.toArray()));
        return sparkSession.createDataFrame(nums, structType);*/
        return dataset;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {

    }

    public void getMessages(List<Row> rows) {
        List<String> messages = new ArrayList<>();
        // skipped 0th index row for header in the feed
        for (int i = 1; i < rows.size(); i++) {
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
        messageProcessor.processMessages(messages);
    }
}
