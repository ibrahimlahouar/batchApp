package com.ilahouar.batchapp.config.table;

import com.ilahouar.batchapp.config.DynamicQueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration générique et réutilisable pour toutes les tables
 * Cette implémentation permet de définir les tables directement dans le YAML
 * sans avoir à créer une classe Java pour chaque table
 */
@Component
@Scope(value = "prototype", proxyMode = ScopedProxyMode.TARGET_CLASS)
@Slf4j
public class DynamicTableConfig extends AbstractTableConfig {
    
    private String tableName;
    private boolean enabled = true;
    
    @Autowired(required = false)
    private DynamicQueryBuilder queryBuilder;
    
    /**
     * Constructeur par défaut requis par Spring
     */
    public DynamicTableConfig() {
        super();
        this.enabled = true;
        // Initialiser des valeurs par défaut pour éviter les nulls
        this.setSourceTableName("");
        this.setTargetTableName("");
        this.setSourceSchema("public");
        this.setTargetSchema("public");
        this.tableName = "";
    }
    
    /**
     * Constructeur pour l'initialisation manuelle
     */
    public DynamicTableConfig(String sourceSchema, String sourceTable, 
                             String targetSchema, String targetTable, 
                             boolean enabled) {
        super(sourceSchema != null ? sourceSchema : "", 
              sourceTable != null ? sourceTable : "", 
              targetSchema != null ? targetSchema : "", 
              targetTable != null ? targetTable : sourceTable != null ? sourceTable : "");
        this.enabled = enabled;
        this.tableName = sourceTable != null ? sourceTable : "";
    }
    
    /**
     * Constructeur complet avec options
     */
    public DynamicTableConfig(String sourceSchema, String sourceTable, 
                             String targetSchema, String targetTable, 
                             boolean enabled, Map<String, Object> options) {
        this(sourceSchema, sourceTable, targetSchema, targetTable, enabled);
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Crée une requête SQL standard pour toutes les tables
     */
    @Override
    public String createSourceQuery() {
        return super.createSourceQuery();
    }
    
    /**
     * Personnalise le reader de manière standard
     */
    @Override
    public JdbcCursorItemReader<Map<String, Object>> customizeReader(JdbcCursorItemReader<Map<String, Object>> reader) {
        // Configuration standard pour toutes les tables
        reader.setFetchSize(1000);
        return reader;
    }
    
    /**
     * Personnalise le writer de manière standard
     */
    @Override
    public JdbcBatchItemWriter<Map<String, Object>> customizeWriter(JdbcBatchItemWriter<Map<String, Object>> writer) {
        // Pas de personnalisation, utiliser le comportement standard
        return writer;
    }
} 