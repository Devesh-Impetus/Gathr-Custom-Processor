package com.yourcompany.component.ss.emitter;

import com.streamanalytix.framework.api.spark.emitter.CustomSSEmitter;
import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.component.ss.emitter.util.FPAggregator;
import com.yourcompany.component.ss.emitter.util.MessageProcessor;
import com.yourcompany.component.ss.emitter.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FPAAAIngressHbaseEmitter implements CustomSSEmitter {

    private static final Log LOGGER = LogFactory.getLog(FPAAAIngressHbaseEmitter.class);

    public Dataset<Row> execute(Dataset<Row> dataset, Map<String, Object> configMap, long batchId) {
        LOGGER.info("inside execute method SampleCustomEmitter");
        getMessages(dataset.collectAsList());
       /* StructType structType = new StructType();
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
        }*/
        dataset.show();
        return dataset;
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
                message.append(appId).append(Constants.MESSAGE_SEPARATOR).append(appId).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(activityId)) {
                message.append(Constants.USER_EVENT_CODE).append(Constants.MESSAGE_SEPARATOR).append(activityId).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(clientIp)) {
                message.append(Constants.IPADDDRESS_CODE).append(Constants.MESSAGE_SEPARATOR).append(Util.ipToLong(clientIp)).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(mobile)) {
                message.append(Constants.MOBILE_CODE).append(Constants.MESSAGE_SEPARATOR).append(mobile).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(serverIp)) {
                message.append(Constants.SERVER_IP_CODE).append(Constants.MESSAGE_SEPARATOR).append(Util.ipToLong(serverIp))
                        .append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(imsi)) {
                message.append(Constants.IMSI_CODE).append(Constants.MESSAGE_SEPARATOR).append(imsi).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(imei)) {
                message.append(Constants.IMEI_CODE).append(Constants.MESSAGE_SEPARATOR).append(imei).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(txType)) {
                message.append(Constants.TX_CODE).append(Constants.MESSAGE_SEPARATOR).append(txType).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(mavenId)) {
                message.append(Constants.MAVEN_ID_CODE).append(Constants.MESSAGE_SEPARATOR).append(mavenId).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(ifId)) {
                message.append(Constants.IF_ID_CODE).append(Constants.MESSAGE_SEPARATOR).append(ifId).append(Constants.MESSAGE_SEPARATOR);
            }
            if (Util.isNotNullOrEmpty(transactionEndTime)) {
                message.append(Constants.TIME_EVENT_CODE).append(Constants.MESSAGE_SEPARATOR).append(transactionEndTime).append(Constants.MESSAGE_SEPARATOR);
            }
            messages.add(message.substring(0, message.length() - 1));
        }

        MessageProcessor messageProcessor = new FPAggregator();
        messageProcessor.processMessages(messages);
    }

}
