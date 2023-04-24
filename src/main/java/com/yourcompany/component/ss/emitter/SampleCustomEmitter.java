/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/

package com.yourcompany.component.ss.emitter;

import com.streamanalytix.framework.api.spark.emitter.CustomEmitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class SampleCustomEmitter implements CustomEmitter {
    private static final Log LOGGER = LogFactory.getLog(SampleCustomEmitter.class);

    public Dataset<Row> execute(Dataset<Row> ds, Map<String, Object> configMap, long batchId) {
        LOGGER.info("inside execute method SampleCustomEmitter");
        ds.show();
        return ds;
    }

    @Override
    public void init(Map<String, Object> map) {
        LOGGER.info("inside execute method SampleCustomEmitter");
    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataset) throws Exception {
        return null;
    }

    @Override
    public void cleanup() {

    }
}
