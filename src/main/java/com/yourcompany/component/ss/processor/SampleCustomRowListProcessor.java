package com.yourcompany.component.ss.processor;

import com.streamanalytix.framework.api.spark.processor.CustomRowListProcessor;
import com.yourcompany.component.ss.common.Constants;
import com.yourcompany.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The Class SampleCustomProcessor.
 */
public class SampleCustomRowListProcessor implements CustomRowListProcessor {

    public static final String MESSAGE_SEPARATOR = "143";
    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611540615487277784L;
    /**
     * The Constant LOGGER.
     */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomRowListProcessor.class);
    private char messageSeparator;

    public SampleCustomRowListProcessor() {
        messageSeparator = (char) Integer.parseInt(MESSAGE_SEPARATOR);
        //messageSeparator = '-';
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.error("inside init of SampleCustomRowListProcessor version1.");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
        LOGGER.error("inside init of SampleCustomRowListProcessor version1. host" + host);

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
    @Override
    public List<Row> process(List<Row> rows) throws Exception {
        LOGGER.error("inside process of SampleCustomRowListProcessor version1");
        List<Row> finalList = new ArrayList<Row>();


        List<Row> nums = new ArrayList<>();

        for (Row row : rows) {
            List<Object> values = new ArrayList<>();
            StructType structType = new StructType();
            String[] columns = row.schema().fieldNames();
            Arrays.sort(columns);
            StringBuilder hbaseColumn = new StringBuilder();
            StringBuilder hbaseColumnVal = new StringBuilder();
            String clientIp = Arrays.binarySearch(columns, "CLIENTIP") >= 0 ? row.getAs("CLIENTIP").toString() : "";
            String mobileNumber = Arrays.binarySearch(columns, "MOBILENUMBER") >= 0 ? row.getAs("MOBILENUMBER").toString() : "";
            String imsi = Arrays.binarySearch(columns, "IMSI") >= 0 ? row.getAs("IMSI").toString() : "";
            String imei = Arrays.binarySearch(columns, "IMEI") >= 0 ? row.getAs("IMEI").toString() : "";

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
                hbaseColumn.append(row.getAs("APPLICATION").toString());
            } else {
                hbaseColumn.append("cf");
            }
            hbaseColumn.append(":")
                    .append(System.currentTimeMillis() / 1000)
                    .append("_")
                    .append(Util.getUniqueNumber());

            values.add(hbaseColumnVal.toString());
            // structType = structType.add(hbaseColumn.toString(), DataTypes.StringType, false);
            GenericRowWithSchema rowWithSchema = new GenericRowWithSchema(values.toArray(), structType);
            GenericRow finalRow = new GenericRow(values.toArray());
            //  finalRow.schema().add("fingerprinting_data", DataTypes.StringType, Boolean.TRUE, Metadata.empty());
            finalList.add(finalRow);
        }
        LOGGER.error("exit process of SampleCustomRowListProcessor version1");
        return finalList;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.error("inside cleanup of SampleCustomRowListProcessor version1");
    }


}
