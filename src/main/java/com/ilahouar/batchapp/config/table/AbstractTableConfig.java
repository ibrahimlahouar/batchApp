package com.ilahouar.batchapp.config.table;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;

/**
 * Implémentation abstraite de base pour les configurations de table
 * Facilite la création de configurations pour de nouvelles tables
 */
@Getter
@Setter
public abstract class AbstractTableConfig implements TableConfig {
    
    @Value("${batch.enabled:true}")
    private boolean enabled;
    
    private String sourceSchema;
    private String targetSchema;
    private String sourceTableName;
    private String targetTableName;
    
    /**
     * Constructeur par défaut
     */
    protected AbstractTableConfig() {
        // Constructeur vide pour Spring
    }
    
    /**
     * Constructeur avec paramètres de base
     */
    protected AbstractTableConfig(String sourceTableName, String targetTableName) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
    }
    
    /**
     * Constructeur complet
     */
    protected AbstractTableConfig(String sourceSchema, String sourceTableName, 
                                 String targetSchema, String targetTableName) {
        this.sourceSchema = sourceSchema;
        this.sourceTableName = sourceTableName;
        this.targetSchema = targetSchema;
        this.targetTableName = targetTableName;
    }
    
    @Override
    public String getJobName() {
        String sourceName = sourceTableName;
        if (sourceSchema != null && !sourceSchema.isEmpty()) {
            sourceName = sourceSchema + "_" + sourceTableName;
        }
        
        String targetName = targetTableName;
        if (targetSchema != null && !targetSchema.isEmpty()) {
            targetName = targetSchema + "_" + targetTableName;
        }
        
        return String.format("%s_to_%s_job", sourceName, targetName);
    }
} 