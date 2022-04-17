package com.mts.metric.spark.util;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ResourceUtil {
    private ResourceUtil() {
        // no op
    }

    public static Resource[] getResources(ResourcePatternResolver resourcePatternResolver, String pattern) {
        try {
            return resourcePatternResolver.getResources(pattern);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String readContent(Resource resource) {
        try (Reader reader = new InputStreamReader(resource.getInputStream(), UTF_8)) {
            return FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
