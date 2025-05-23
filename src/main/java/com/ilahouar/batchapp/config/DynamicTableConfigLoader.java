package com.ilahouar.batchapp.config;

import com.ilahouar.batchapp.config.table.DynamicTableConfig;
import com.ilahouar.batchapp.config.table.TableConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Charge les configurations de tables depuis le YAML
 */
@Configuration
@Slf4j
public class DynamicTableConfigLoader {

    private final TablesProperties tablesProperties;
    
    @Autowired
    public DynamicTableConfigLoader(TablesProperties tablesProperties) {
        this.tablesProperties = tablesProperties;
    }
    
    /**
     * Crée dynamiquement les configurations de tables à partir des propriétés
     */
    @Bean
    public List<TableConfig> dynamicTableConfigs() {
        List<TableConfig> tableConfigs = new ArrayList<>();
        
        if (tablesProperties.getTables() == null || tablesProperties.getTables().isEmpty()) {
            log.warn("Aucune table définie dans la configuration. Assurez-vous de définir les tables dans le fichier application.yml");
            return tableConfigs;
        }
        
        log.info("Chargement de {} définitions de tables depuis la configuration", tablesProperties.getTables().size());
        
        for (Map.Entry<String, TableProperties> entry : tablesProperties.getTables().entrySet()) {
            String tableName = entry.getKey();
            TableProperties props = entry.getValue();
            
            // Si sourceTable n'est pas défini, utiliser la clé comme nom de table
            if (props.getSourceTable() == null || props.getSourceTable().isEmpty()) {
                props.setSourceTable(tableName);
            }
            
            // Si targetTable n'est pas défini, utiliser le même nom que sourceTable
            if (props.getTargetTable() == null || props.getTargetTable().isEmpty()) {
                props.setTargetTable(props.getSourceTable());
            }
            
            // Créer la configuration dynamique pour cette table - traitement standard
            DynamicTableConfig config = new DynamicTableConfig(
                props.getSourceSchema(),
                props.getSourceTable(),
                props.getTargetSchema(),
                props.getTargetTable(),
                props.isEnabled(),
                null // Pas d'options spécifiques, toutes les tables sont traitées de la même manière
            );
            
            tableConfigs.add(config);
            log.info("Table configurée: {} -> {} (schéma source: {}, schéma cible: {})", 
                    config.getSourceTableName(), 
                    config.getTargetTableName(),
                    config.getSourceSchema(),
                    config.getTargetSchema());
        }
        
        log.info("{} tables configurées avec succès", tableConfigs.size());
        return tableConfigs;
    }
    
    /**
     * Conteneur pour les propriétés de tables
     */
    @Component
    @ConfigurationProperties(prefix = "batch")
    @Data
    public static class TablesProperties {
        private Map<String, TableProperties> tables = new HashMap<>();
    }
    
    /**
     * Structure des propriétés d'une table
     */
    @Data
    public static class TableProperties {
        private String sourceSchema = "";
        private String sourceTable;
        private String targetSchema = "";
        private String targetTable;
        private boolean enabled = true;
    }
} 