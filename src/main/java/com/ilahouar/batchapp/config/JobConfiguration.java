package com.ilahouar.batchapp.config;

import com.ilahouar.batchapp.config.table.DynamicTableConfig;
import com.ilahouar.batchapp.config.table.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration des jobs Spring Batch
 */
@Configuration
@Slf4j
public class JobConfiguration {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TableJobFactory tableJobFactory;
    private final DynamicTableConfigLoader.TablesProperties tablesProperties;
    
    @Autowired
    public JobConfiguration(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            TableJobFactory tableJobFactory,
            DynamicTableConfigLoader.TablesProperties tablesProperties) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.tableJobFactory = tableJobFactory;
        this.tablesProperties = tablesProperties;
    }

    /**
     * Crée un map des jobs en fonction des configurations de tables chargées depuis le YAML
     */
    @Bean(name = "tableJobs")
    public Map<String, Job> tableJobs() {
        Map<String, Job> jobs = new HashMap<>();
        List<TableConfig> configs = loadTableConfigs();
        
        log.info("Initialisation des jobs pour {} configurations de tables", configs.size());
        
        for (TableConfig config : configs) {
            if (!config.isEnabled()) {
                log.info("La table {} est désactivée, ignorée", config.getSourceTableName());
                continue;
            }
            
            log.info("Configuration de la table {} -> {} (Schéma source: {}, Schéma cible: {})",
                    config.getSourceTableName(), 
                    config.getTargetTableName(), 
                    config.getSourceSchema(), 
                    config.getTargetSchema());
            
            try {
                // Utilisation du factory pour créer un job avec un traitement standard
                Job job = tableJobFactory.createJobForTable(config);
                jobs.put(config.getJobName(), job);
                log.info("Job {} créé avec succès", config.getJobName());
            } catch (Exception e) {
                log.error("Erreur lors de la création du job pour {}: {}", config.getSourceTableName(), e.getMessage());
                throw new RuntimeException("Impossible de créer la table cible", e);
            }
        }
        
        log.info("{} jobs créés et enregistrés", jobs.size());
        return jobs;
    }
    
    /**
     * Charge les configurations de tables depuis le YAML avec un traitement standard
     */
    private List<TableConfig> loadTableConfigs() {
        List<TableConfig> tableConfigs = new ArrayList<>();
        
        if (tablesProperties.getTables() == null || tablesProperties.getTables().isEmpty()) {
            log.warn("Aucune table définie dans la configuration");
            return tableConfigs;
        }
        
        for (Map.Entry<String, DynamicTableConfigLoader.TableProperties> entry : 
                tablesProperties.getTables().entrySet()) {
            String tableName = entry.getKey();
            DynamicTableConfigLoader.TableProperties props = entry.getValue();
            
            // Si sourceTable n'est pas défini, utiliser la clé comme nom de table
            if (props.getSourceTable() == null || props.getSourceTable().isEmpty()) {
                props.setSourceTable(tableName);
            }
            
            // Si targetTable n'est pas défini, utiliser le même nom que sourceTable
            if (props.getTargetTable() == null || props.getTargetTable().isEmpty()) {
                props.setTargetTable(props.getSourceTable());
            }
            
            // Créer la configuration dynamique pour cette table avec un traitement standard
            DynamicTableConfig config = new DynamicTableConfig(
                props.getSourceSchema(),
                props.getSourceTable(),
                props.getTargetSchema(),
                props.getTargetTable(),
                props.isEnabled(),
                null // Pas d'options, traitement standard pour toutes les tables
            );
            
            tableConfigs.add(config);
        }
        
        return tableConfigs;
    }
} 