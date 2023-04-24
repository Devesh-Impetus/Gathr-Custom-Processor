package com.yourcompany.component.ss.emitter.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniqueKeyGenerator {

    static Logger log = LoggerFactory.getLogger(UniqueKeyGenerator.class.getName());
    private static long startIndex;
    private static long endIndex;
    private static UniqueKeyGenerator uniqueKeyGenerator;

    private UniqueKeyGenerator() {
        UniqueKeyGenerator.startIndex = 100000;
        UniqueKeyGenerator.endIndex = 199999;
    }

    public synchronized static UniqueKeyGenerator getInstance() {
        if (uniqueKeyGenerator == null) {
            uniqueKeyGenerator = new UniqueKeyGenerator();
            return uniqueKeyGenerator;
        } else {
            return uniqueKeyGenerator;
        }

    }

    public synchronized String getUniqueKey(String key) {
        String ukey = "";
        if (startIndex < endIndex) {
            ukey = key + "_" + startIndex;
            startIndex++;
        } else {
            startIndex = 100000;
            ukey = key + "_" + startIndex;
            startIndex++;
        }
        return ukey;
    }
    /*public synchronized String getUniqueKey(String key) {
        return key + "_" + Util.getUniqueNumber();
    }*/
}