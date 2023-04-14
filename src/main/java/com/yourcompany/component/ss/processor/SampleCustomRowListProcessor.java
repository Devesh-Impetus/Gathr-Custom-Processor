package com.yourcompany.component.ss.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import com.streamanalytix.framework.api.spark.processor.CustomRowListProcessor;
import com.yourcompany.component.ss.common.Constants;

/** The Class SampleCustomProcessor. */
public class SampleCustomRowListProcessor implements CustomRowListProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 611540615487277784L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomRowListProcessor.class);

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
        for (Row row : rows) {
            List<Object> values = new ArrayList<Object>();
            String[] fields = row.schema().fieldNames();
            for (int i = 0; i < fields.length; i++) {
                values.add(row.get(i));
            }
            GenericRow finalRow = new GenericRow(values.toArray());
            finalList.add(finalRow);
        }
        LOGGER.error("exit process of SampleCustomRowListProcessor version1");
        return finalList;
        // put some custom logic here
        // return dataset
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
