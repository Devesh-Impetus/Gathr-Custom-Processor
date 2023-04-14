package com.yourcompany.component.ss.processor;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.streamanalytix.framework.api.spark.processor.CustomDatasetsProcessor;
import com.yourcompany.component.ss.common.Constants;

/** The Class SampleCustomDatasetsProcessor. */
public class SampleCustomDatasetsProcessor implements CustomDatasetsProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 611540615477277766L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomDatasetsProcessor.class);

    String outputDataset = null;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.info("inside init of SampleCustomDatasetsProcessor version1.");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        outputDataset = (String) connectionConfig.get("outputDataset");
        LOGGER.info("inside init of SampleCustomDatasetsProcessor, outputDataset component id - " + outputDataset);

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
    @Override
    public Dataset<Row> process(Map<String, Dataset<Row>> inputDataset) throws Exception {
        LOGGER.info("Inside process of SampleCustomDatasetsProcessor, outputDataset : " + outputDataset + ", inputDataset map : " + inputDataset);
        return inputDataset.get(outputDataset);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.error("inside cleanup of SampleCustomDatasetsProcessor");
    }

}