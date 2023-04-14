package com.yourcompany.component.ss.channel;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.streamanalytix.framework.api.spark.channel.AbstractChannel;
import com.yourcompany.component.ss.common.Constants;

/** The Class SampleSparkSourceChannel. */
public class SampleSparkSourceChannel extends AbstractChannel {
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -1271354425951397508L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SampleSparkSourceChannel.class);

    /** The host name. */
    private String hostName;
    /** The port. */
    private int port;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.AbstractChannel#init(java.util .Map)
     */
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.info("conf is : " + conf);
        // All the additional property set from UI will be fetched from this map
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        hostName = (String) connectionConfig.get(Constants.HOST_NAME);
        port = Integer.valueOf((String) connectionConfig.get(Constants.PORT));
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDataset(org.apache.spark.sql.SparkSession)
     */
    @Override
    public Dataset<Row> getDataset(SparkSession spark) {
        return spark.readStream().format("socket").option("host", hostName).option("port", port).load();
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.AbstractChannel#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.info("In cleanup()");
    }
}