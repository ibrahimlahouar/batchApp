package com.ilahouar.batchapp.config.table;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;

import java.util.Map;

/**
 * Interface de base pour les configurations de tables spécifiques
 */
public interface TableConfig {
    
    /**
     * Retourne le nom de la table source dans Trino
     */
    String getSourceTableName();
    
    /**
     * Retourne le nom de la table cible dans PostgreSQL
     */
    String getTargetTableName();
    
    /**
     * Retourne le schéma source dans Trino
     */
    String getSourceSchema();
    
    /**
     * Retourne le schéma cible dans PostgreSQL
     */
    String getTargetSchema();
    
    /**
     * Crée une requête SQL personnalisée pour lire les données de la source
     */
    default String createSourceQuery() {
        if (getSourceSchema() != null && !getSourceSchema().isEmpty()) {
            return String.format("SELECT * FROM %s.%s", getSourceSchema(), getSourceTableName());
        }
        return String.format("SELECT * FROM %s", getSourceTableName());
    }
    
    /**
     * Personnaliser le reader si nécessaire
     */
    default JdbcCursorItemReader<Map<String, Object>> customizeReader(JdbcCursorItemReader<Map<String, Object>> reader) {
        return reader;
    }
    
    /**
     * Personnaliser le writer si nécessaire
     */
    default JdbcBatchItemWriter<Map<String, Object>> customizeWriter(JdbcBatchItemWriter<Map<String, Object>> writer) {
        return writer;
    }
    
    /**
     * Nom unique pour ce job de table
     */
    default String getJobName() {
        return String.format("%s_to_%s_job", getSourceTableName(), getTargetTableName());
    }
    
    /**
     * Indique si cette configuration est activée
     */
    boolean isEnabled();
} 