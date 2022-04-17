package com.mts.metric.spark.listener;

import com.mts.metric.spark.service.EnricherServiceFacade;
import com.mts.metric.spark.service.EnrichmentFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static com.mts.metric.spark.config.Profiles.PROD;

@Component
@Profile(PROD)
public class MainListener {
    private static final Logger LOG = LoggerFactory.getLogger(MainListener.class);

    private final EnricherServiceFacade enricherServiceFacade;

    public MainListener(EnricherServiceFacade enricherServiceFacade) {
        this.enricherServiceFacade = enricherServiceFacade;
    }

    @EventListener
    public void proceedEnrichment(ContextRefreshedEvent event) {
        final long startNano = System.nanoTime();
        LOG.info("Starting enrichment process");
        try {
            enricherServiceFacade.proceedEnrichment();
            LOG.info("Enrichment has finished successfully. It took " + Duration.ofNanos(System.nanoTime() - startNano));
        } catch (Exception e) {
            String err = "Enrichment has finished with error. It took " + Duration.ofNanos(System.nanoTime() - startNano);
            LOG.error(err, e);
            throw new EnrichmentFailedException(err, e);
        }
    }
}
