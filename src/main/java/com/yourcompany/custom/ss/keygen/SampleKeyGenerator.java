package com.yourcompany.custom.ss.keygen;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

import com.streamanalytix.framework.api.keygen.IKeyGenerator;

public class SampleKeyGenerator implements IKeyGenerator {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 8190287831164952130L;

    @Override
    public String getKey(Map<String, Object> record) {
        String uuid = UUID.randomUUID().toString();
        return DigestUtils.md5Hex(uuid);
    }

}
