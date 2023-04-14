/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/

package com.yourcompany.component.ss.emitter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.streamanalytix.framework.api.spark.emitter.CustomSSEmitter;

public class SampleCustomEmitter implements CustomSSEmitter {

    private static final Log LOGGER = LogFactory.getLog(SampleCustomEmitter.class);

    public Dataset<Row> execute(Dataset<Row> ds, Map<String, Object> configMap, long batchId) {
        LOGGER.info("inside execute method SampleCustomEmitter");
        ds.show();
        return ds;
    }

}
