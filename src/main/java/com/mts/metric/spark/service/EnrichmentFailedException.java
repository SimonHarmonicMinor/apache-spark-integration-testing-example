package com.mts.metric.spark.service;

import java.util.Collection;

public class EnrichmentFailedException extends RuntimeException {
    private static final long serialVersionUID = 6529685098267757690L;

    public EnrichmentFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public EnrichmentFailedException(Collection<EnrichmentFailedException> errors) {
        for (EnrichmentFailedException error : errors) {
            addSuppressed(error);
        }
    }
}
