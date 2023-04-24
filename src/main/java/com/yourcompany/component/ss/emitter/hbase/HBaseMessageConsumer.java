package com.yourcompany.component.ss.emitter.hbase;

import com.yourcompany.component.ss.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Amit Singh Hora
 */
public class HBaseMessageConsumer {

    public static boolean flagIfexception = false;
    private static Logger log = LoggerFactory.getLogger(HBaseMessageConsumer.class
            .getName());

    public static boolean consumeMessagesNew(

            Map<String, Map<Long, String>> aaatotimestamptosetofevent) {
        log.debug("Inside Consume Messages");

        int numberOfThreads = Integer.parseInt(Constants.NUMBER_OF_THREADS);

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        executorService.execute(new HBaseAAAInfoWriterNew(aaatotimestamptosetofevent));

        executorService.shutdown();
        log.debug("Waiting for Exceuter Service Threads to finish");
        while (!executorService.isTerminated()) {
        }

        log.debug("Threads finished working");
        return HBaseMessageConsumer.flagIfexception;

    }
}
