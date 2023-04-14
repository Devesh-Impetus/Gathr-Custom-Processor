/*******************************************************************************
* Copyright StreamAnalytix and Impetus Technologies.
* All rights reserved
******************************************************************************/
package com.yourcompany.custom.ss.partitioner;

import com.streamanalytix.framework.api.spark.partitioner.SAXKafkaPartitioner;

/** The Class SAXKafkaPatitioner. */
public class SampleKafkaPartitioner implements SAXKafkaPartitioner {

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.partitioner.SAXKafkaPartitioner#init()
     */
    @Override
    public void init() {

    }

    /** Partition.
     *
     * @param data
     *            the data byte array
     * @return the int */
    @Override
    public int partition(byte[] data) {

        int partition = 2;
        // System.out.println("Number of partition: "+partition); 
        /*
         * Write a logic here to decide or get the partition number so that data will go to that partition only Note: Make sure returned partition
         * number should not be more than the pipeline defined partition property
         */

        return partition;
    }

}
