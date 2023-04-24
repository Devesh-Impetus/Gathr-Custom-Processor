package com.yourcompany.component.ss.processor;

import com.streamanalytix.framework.api.spark.processor.CustomRowProcessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.xerial.snappy.Snappy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Class SnappyUncompress.
 */
public class SnappyBinaryUncompress implements CustomRowProcessor {

    /**
     * The Constant serialVersionUID.
     */
    private static final long serialVersionUID = 611541615477677284L;

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
        LOGGER.info("inside init of SnappyBinaryUncompress.");
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#process(org.apache.spark.sql.Dataset)
     */
    @Override
    public Row process(Row row) throws Exception {

        List<Object> values = new ArrayList<Object>();
        StructField[] fields = row.schema().fields();
        String value = null;
        for (int itr = 0; itr < fields.length; itr++) {
            values.add(row.get(itr));
            if (fields[itr].dataType().sameType(DataTypes.BinaryType)) {
                byte[] byteArray = (byte[]) row.get(itr);
                byteArray = Snappy.uncompress(byteArray);
                value = new String(byteArray, "UTF-8");
                row.schema().add("decoded_binary_data", DataTypes.StringType, Boolean.TRUE, Metadata.empty());
            }
        }
        if (value != null && !value.isEmpty()) {
            LOGGER.debug("snappy result " + value);
            values.add(value);
        }
        return new GenericRow(values.toArray());
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.CustomProcessor#cleanup()
     */
    @Override
    public void cleanup() {
        LOGGER.error("inside cleanup of SnappyUncompress version1");
    }

}
