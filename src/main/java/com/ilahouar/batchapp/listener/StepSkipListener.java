package com.ilahouar.batchapp.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Listener pour gérer les enregistrements ignorés lors du traitement batch
 */
@Component
@Slf4j
public class StepSkipListener implements SkipListener<Map<String, Object>, Map<String, Object>> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.error("Erreur de lecture, enregistrement ignoré. Erreur: {}", t.getMessage());
        log.debug("Détail de l'erreur: ", t);
    }

    @Override
    public void onSkipInProcess(Map<String, Object> item, Throwable t) {
        log.error("Erreur lors du traitement de l'enregistrement {}, ignoré. Erreur: {}", 
                getRecordIdentifier(item), t.getMessage());
        log.debug("Détail de l'erreur: ", t);
    }

    @Override
    public void onSkipInWrite(Map<String, Object> item, Throwable t) {
        log.error("Erreur lors de l'écriture de l'enregistrement {}, ignoré. Erreur: {}", 
                getRecordIdentifier(item), t.getMessage());
        log.debug("Détail de l'erreur: ", t);
    }
    
    /**
     * Extrait un identifiant pour l'enregistrement à partir de la Map
     */
    private String getRecordIdentifier(Map<String, Object> item) {
        if (item == null) {
            return "null";
        }
        
        // Tente de trouver un ID ou une clé primaire
        if (item.containsKey("id")) {
            return item.get("id").toString();
        } else if (item.containsKey("ID")) {
            return item.get("ID").toString();
        } else {
            // Sinon renvoie les premières entrées de la Map
            return item.toString().substring(0, Math.min(100, item.toString().length()));
        }
    }
} 