package com.yourcompany.component.ss.processor;

import com.streamanalytix.framework.api.spark.processor.CustomRowProcessor;
import com.yourcompany.component.ss.common.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: Auto-generated Javadoc

/**
 * The Class SampleCustomProcessor.
 */
public class SampleCustomRowProcessor implements CustomRowProcessor {

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611540615477677784L;

    /**
     * The Constant LOGGER.
     */
    private static final Log LOGGER = LogFactory.getLog(SampleCustomRowProcessor.class);

    /**
     * The schema.
     */
    StructType schema = new StructType();

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#init(java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> conf) {
        LOGGER.error("inside init of SampleCustomRowProcessor version1.");

        // To get key value pairs if provided in extra configurations at component level, use connnectionConfig key
        Map<String, Object> connectionConfig = (Map<String, Object>) conf.get(Constants.CONNECTION_CONFIG);
        // To get value of key 'host', say
        String host = (String) connectionConfig.get(Constants.HOST);
        LOGGER.error("inside init of SampleCustomRowProcessor version1. host" + host);

        if (conf.containsKey(Constants.SCHEMA)) {
            schema = (StructType) conf.get(Constants.SCHEMA);
        }

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
    @Override
    public Row process(Row row) throws Exception {
        LOGGER.error("inside process of SampleCustomRowProcessor version1");
        List<Object> values = new ArrayList<Object>();
        String[] fields = row.schema().fieldNames();
        StructField[] schemaField = schema.fields();
        boolean isValueAdded;
        for (int k = 0; k < schemaField.length; k++) {
            isValueAdded = false;
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].equalsIgnoreCase(schemaField[k].name())) {
                    values.add(row.get(i));
                    isValueAdded = true;
                    break;
                }
            }
            if (isValueAdded) {
                continue;
            } else {
                // new value should be added as per datatype configured on UI
                values.add("new_value");
            }

        }
        LOGGER.error("exit process of SampleCustomRowProcessor version1");
        return new GenericRow(values.toArray());
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.error("inside cleanup of SampleCustomRowProcessor version1");
    }

}
