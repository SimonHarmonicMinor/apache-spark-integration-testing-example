package com.mts.metric.spark.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class EnricherServiceFacade {
    private final List<EnricherService> enricherServices;

    public EnricherServiceFacade(List<EnricherService> enricherServices) {
        this.enricherServices = enricherServices;
    }

    public void proceedEnrichment() {
        List<EnrichmentFailedException> errors = new ArrayList<>();
        for (EnricherService service : enricherServices)
            try {
                service.proceedEnrichment();
            } catch (Exception e) {
                errors.add(new EnrichmentFailedException("Unexpected error during enrichment processing", e));
            }
        if (!errors.isEmpty()) {
            throw new EnrichmentFailedException(errors);
        }
    }
}
