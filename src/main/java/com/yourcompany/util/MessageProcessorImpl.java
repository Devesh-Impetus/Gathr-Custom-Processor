package com.yourcompany.util;

import java.util.List;

public class MessageProcessorImpl implements MessageProcessor {
    private static MessageProcessor messageProcessor;


    public MessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    @Override
    public void processMessages(List<String> listOfMessageString) {
        messageProcessor.processMessages(listOfMessageString);
    }
}
