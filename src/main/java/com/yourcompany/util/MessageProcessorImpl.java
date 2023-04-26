package com.yourcompany.util;

import java.util.List;
import java.util.Map;

public class MessageProcessorImpl implements MessageProcessor{
    private static  MessageProcessor messageProcessor;


    public MessageProcessor getMessageProcessor() {
        return messageProcessor;
    }
    
    @Override
    public Map<String, String> processMessages(List<String> listOfMessageString) {
        return messageProcessor.processMessages(listOfMessageString);
    }
    
	@Override
	public Map<String, String> processMessagesForFPIngress(List<String> listOfMessageString) {
        return messageProcessor.processMessages(listOfMessageString);
	}
}
