package com.yourcompany.util;

import java.util.List;
import java.util.Map;

public class MessageProcessorImpl implements MessageProcessor {
    private static MessageProcessor messageProcessor;


    public MessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    @Override
    public Map<String, Map<String, Map<String, String>>> processMessages(List<String> listOfMessageString) {
        return messageProcessor.processMessages(listOfMessageString);
    }
}
