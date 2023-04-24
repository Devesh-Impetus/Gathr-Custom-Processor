package com.yourcompany.util;

import java.util.List;
import java.util.Map;

public interface MessageProcessor {
    public Map<String, String> processMessages(List<String> listOfMessageString);
}
