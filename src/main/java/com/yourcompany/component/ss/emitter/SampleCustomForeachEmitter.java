/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/

package com.yourcompany.component.ss.emitter;

import com.streamanalytix.framework.api.spark.emitter.CustomForeachEmitter;
import com.yourcompany.component.ss.common.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The Class SampleCustomForeachEmitter.
 */
public class SampleCustomForeachEmitter implements CustomForeachEmitter {

    private static final Log LOGGER = LogFactory.getLog(SampleCustomForeachEmitter.class);

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 3627163059367911082L;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.info("inside init of SampleCustomForeachEmitter");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.emitter.SampleCustomForeachEmitter#open(long, long)
     */
    @Override
    public boolean open(long arg0, long arg1) {
        LOGGER.info("inside open of SampleCustomForeachEmitter.");
        return true;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.emitter.CustomForeachEmitter#process(org.apache.spark.sql.Row, org.apache.spark.sql.SQLContext)
     */
    @Override
    public void process(Row row) {
        LOGGER.info("inside process of SampleCustomForeachEmitter.");
        List<String> str = Arrays.asList(row.schema().fieldNames());
        Map<String, Object> dataMap = JavaConverters.mapAsJavaMapConverter(row.getValuesMap(getSeqString(str))).asJava();
        LOGGER.info(dataMap);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.emitter.SampleCustomForeachEmitter#close(java.lang.Throwable)
     */
    @Override
    public void close(Throwable arg0) {
        LOGGER.info("inside close of SampleCustomForeachEmitter.");
    }

    /**
     * Get Seq from list.
     *
     * @param list the list
     * @return the seq
     */
    private Seq<String> getSeqString(List<String> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.info("inside cleanup of SampleCustomForeachEmitter.");
    }

}
