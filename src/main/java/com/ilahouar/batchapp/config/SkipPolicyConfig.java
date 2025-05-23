package com.ilahouar.batchapp.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

/**
 * Configuration de la politique de gestion des erreurs et des sauts
 */
@Configuration
@Slf4j
public class SkipPolicyConfig {

    @Value("${batch.max-skips}")
    private int maxSkips;

    /**
     * Définit quand ignorer une erreur et continuer le traitement
     * Cette politique permet de gérer la résilience du batch
     */
    @Bean
    public SkipPolicy skipPolicy() {
        return (throwable, skipCount) -> {
            // Si le nombre maximum de sauts est dépassé, on arrête d'ignorer les erreurs
            if (skipCount > maxSkips) {
                log.warn("Nombre maximum de sauts atteint ({}), arrêt des sauts", maxSkips);
                return false;
            }

            // Ignorer les erreurs de base de données et certaines erreurs fonctionnelles
            if (throwable instanceof SQLException ||
                    throwable instanceof NumberFormatException ||
                    throwable instanceof IllegalArgumentException) {
                log.warn("Enregistrement ignoré à cause de: {}", throwable.getMessage());
                return true;
            }
            
            // Ne pas ignorer les autres types d'erreurs
            log.error("Erreur non ignorable: {}", throwable.getMessage());
            return false;
        };
    }
} 