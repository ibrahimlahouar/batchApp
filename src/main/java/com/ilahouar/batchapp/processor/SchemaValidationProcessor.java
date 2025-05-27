package com.ilahouar.batchapp.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Processeur minimal qui transmet les données sans transformation
 */
@Component
@StepScope
@Slf4j
public class SchemaValidationProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final String targetTable;
    
    public SchemaValidationProcessor(
            @Qualifier("oracleDataSource") DataSource oracleDataSource,
            @Value("#{jobParameters['targetTable'] ?: '${batch.oracle.table-name:default_table}'}") String targetTable) {
        this.targetTable = targetTable;
        log.info("Initialisation du processeur pour la table Oracle: {}", targetTable);
    }

    @Override
    public Map<String, Object> process(Map<String, Object> sourceItem) throws Exception {
        if (sourceItem == null) {
            return null;
        }
        
        // Aucune transformation - transmission directe
        return sourceItem;
    }
} 