package com.yourcompany.component.ss.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.streamanalytix.framework.api.spark.processor.CustomProcessor;
import com.yourcompany.component.ss.common.Constants;

/** The Class SampleCustomProcessor. */
public class SampleCustomProcessor implements CustomProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 611540615477277784L;
    public static final String FP_SCHEMA_FILE_NAME="fingerprintingSchemaFields.properties";

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomProcessor.class);

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
        LOGGER.error("inside process of SampleCustomProcessor version1");
        LOGGER.error("Removing duplicate input rows version1");
        List<Row> dataset = datasetIn.collectAsList();
        List<String> fingerprintingSchemaFields = new ArrayList<>();
        try (InputStream propInStr = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(FP_SCHEMA_FILE_NAME)) {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(propInStr));
            String line;
            while ((line = rdr.readLine()) != null) {
                fingerprintingSchemaFields.add(line);
            }
            System.out.println(fingerprintingSchemaFields);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // put some custom logic here
        Dataset<Row> datasetModified = datasetIn.dropDuplicates();
        LOGGER.error("exit process of SampleCustomProcessor version1");
        // return dataset
        return datasetModified;
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
