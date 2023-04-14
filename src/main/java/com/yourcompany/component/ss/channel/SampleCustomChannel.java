/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/
package com.yourcompany.component.ss.channel;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.framework.api.spark.channel.source.BaseSource;
import com.yourcompany.component.ss.common.Constants;

/** The Class SampleCustomChannel. */
public class SampleCustomChannel extends BaseSource {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 455062295979078615L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomChannel.class);

    /** The Constant INIT_COMP_ERROR. */
    private static final String INIT_COMP_ERROR = "Error in initializing input components";

    /** The socket. */
    private transient Socket socket;

    /** The reciever. */
    private transient Thread reciever;

    /** The data list. */
    private BlockingQueue<String> dataList;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        dataList = new LinkedBlockingQueue<String>();
        try {
            // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
            Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);

            socket = new Socket((String) connectionConfig.get(Constants.HOST_NAME), Integer.valueOf((String) connectionConfig.get(Constants.PORT)));
            asyncConsume();

            // To get value of key 'otherKey', say
            String otherKey = (String) connectionConfig.get("otherKey");

        } catch (Exception e) {
            LOGGER.error(INIT_COMP_ERROR, e);
        }
    }

    /** Async consume. */
    private void asyncConsume() {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            reciever = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            String line = reader.readLine();
                            if (line == null || line.isEmpty()) {
                                return;
                            }
                            SampleCustomChannel.this.dataList.add(line);
                        } catch (SocketException e) {
                            LOGGER.error(INIT_COMP_ERROR, e);
                            return;
                        } catch (Exception e) {
                            LOGGER.error(INIT_COMP_ERROR, e);
                        }
                    }
                }
            };
            reciever.setDaemon(true);
            reciever.start();
        } catch (Exception e) {
            LOGGER.error(INIT_COMP_ERROR, e);
        }
    }

    @Override
    public List<String> receive() {
        List<String> list = new ArrayList<String>();
        try {
            if (!dataList.isEmpty()) {
                int cnt = dataList.drainTo(list);
                LOGGER.debug("Number of records recieved: " + cnt);
            }
        } catch (Exception e) {
            LOGGER.error("Error in consuming data", e);
        }
        return list;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void cleanup() {
        LOGGER.info("In cleanup()");
        if (reciever != null) {
            reciever.stop();
        }
        close(socket);
    }

    /** Close.
     *
     * @param closeable
     *            the closeable */
    private void close(Closeable closeable) {
        LOGGER.info("In closing input components");
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.error("Error in closing input components", e);
            }
        }
    }
}
