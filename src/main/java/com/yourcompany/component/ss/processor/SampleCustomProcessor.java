package com.yourcompany.component.ss.processor;

import com.streamanalytix.framework.api.spark.processor.CustomProcessor;
import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The Class SampleCustomProcessor.
 */
public class SampleCustomProcessor implements CustomProcessor {

    public static final String FP_SCHEMA_FILE_NAME = "fingerprintingSchemaFields.properties";
    public static final String MESSAGE_SEPARATOR = "143";
    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611540615477277784L;
    /**
     * The Constant LOGGER.
     */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomProcessor.class);
    private char messageSeparator;

    public SampleCustomProcessor() {
        // messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
        messageSeparator = '-';
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.info("inside init of SampleCustomProcessor version1.");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
        LOGGER.error("inside init of SampleCustomProcessor version1. host" + host);

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
    @Override
    public Dataset<Row> process(Dataset<Row> datasetIn) throws Exception {
        LOGGER.error("inside process of SampleCustomProcessor for FPIngress");

        SparkSession sparkSession = datasetIn.sparkSession();
        List<Row> dataset = datasetIn.collectAsList();
        String[] columns = datasetIn.schema().fieldNames();
        StructType structType = new StructType();
        List<Row> nums = new ArrayList<>();
        Arrays.sort(columns);

        for (int i = 0; i < dataset.size(); i++) {
            StringBuilder hbaseColumn = new StringBuilder();
            StringBuilder hbaseColumnVal = new StringBuilder();
            String clientIp = Arrays.binarySearch(columns, "SERVERIP") >= 0 ? dataset.get(i).getAs("SERVERIP") : "";
            String mobileNumber = Arrays.binarySearch(columns, "MOBILENUMBER") >= 0 ? dataset.get(i).getAs("MOBILENUMBER") : "";
            String imsi = Arrays.binarySearch(columns, "IMSI") >= 0 ? dataset.get(i).getAs("IMSI") : "";
            String imei = Arrays.binarySearch(columns, "IMEI") >= 0 ? dataset.get(i).getAs("IMEI") : "";

            if (Util.isNotNullOrEmpty(clientIp)) {
                if (clientIp.contains(":")) {
                    hbaseColumnVal.append(clientIp).append(messageSeparator);
                } else {
                    hbaseColumnVal.append(Util.ipToLong(clientIp)).append(messageSeparator);
                }
            } else {
                hbaseColumnVal.append(messageSeparator);
            }
            if (Util.isNotNullOrEmpty(mobileNumber)) {
                hbaseColumnVal.append(mobileNumber).append(messageSeparator);
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
            hbaseColumnVal = new StringBuilder(hbaseColumnVal.substring(0, hbaseColumnVal.length() - 1));
            nums.add(RowFactory.create(hbaseColumnVal.toString()));

            if (Arrays.binarySearch(columns, "APPLICATION") >= 0) {
                hbaseColumn.append(dataset.get(i).getAs("APPLICATION").toString());
            } else {
                hbaseColumn.append("cf");
            }
            hbaseColumn.append(":")
                    .append(System.currentTimeMillis() / 1000)
                    .append("_")
                    .append(Util.getUniqueNumber());

            structType = structType.add(hbaseColumn.toString(), DataTypes.StringType, false);
        }
        /*StructType s = datasetIn.schema();
        structType = structType.add("test", DataTypes.StringType, false);
        GenericRowWithSchema rowWithSchema = new GenericRowWithSchema();
*/
        return sparkSession.createDataFrame(nums, structType);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.error("inside cleanup of SampleCustomProcessor version1");
    }
}
