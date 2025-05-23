package com.ilahouar.batchapp.launcher;

import com.ilahouar.batchapp.config.DynamicTableConfigLoader;
import com.ilahouar.batchapp.config.table.TableConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Lance automatiquement tous les jobs configurés au démarrage de l'application
 */
@Component
@Slf4j
public class BatchLauncher {

    private final JobLauncher jobLauncher;
    private final DynamicTableConfigLoader tableConfigLoader;
    
    @Qualifier("tableJobs")
    private final Map<String, Job> tableJobs;

    @Autowired
    public BatchLauncher(JobLauncher jobLauncher, 
                        DynamicTableConfigLoader tableConfigLoader,
                        @Qualifier("tableJobs") Map<String, Job> tableJobs) {
        this.jobLauncher = jobLauncher;
        this.tableConfigLoader = tableConfigLoader;
        this.tableJobs = tableJobs;
    }

    /**
     * Exécuté automatiquement au démarrage de l'application
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info(">>> Event ApplicationReadyEvent déclenché - BatchLauncher activé");
        
        // Récupérer les tableConfigs directement à partir du chargeur
        List<TableConfig> configsFromLoader = tableConfigLoader.dynamicTableConfigs();
        log.info(">>> tableConfigs.size = {}", configsFromLoader.size());
        log.info(">>> tableJobs.size = {}", tableJobs.size());
        
        // Afficher détails des tableConfigs
        for (TableConfig config : configsFromLoader) {
            log.info(">>> Config: source={}, target={}, enabled={}, nom job={}",
                    config.getSourceTableName(),
                    config.getTargetTableName(),
                    config.isEnabled(),
                    config.getJobName());
        }
        
        // Afficher détails des jobs
        log.info(">>> Jobs disponibles: {}", tableJobs.keySet());
        
        try {
            launchAllJobsInOrder(configsFromLoader);
        } catch (Exception e) {
            log.error("Erreur lors du lancement des jobs: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Lance tous les jobs configurés dans l'ordre défini dans le YAML
     */
    private void launchAllJobsInOrder(List<TableConfig> tableConfigs) throws Exception {
        // Filtrer les tables désactivées et avec des noms vides
        List<TableConfig> validConfigs = tableConfigs.stream()
                .filter(TableConfig::isEnabled)
                .filter(config -> config.getSourceTableName() != null && !config.getSourceTableName().isEmpty())
                .collect(Collectors.toList());
        
        log.info(">>> Après filtrage: {} configs valides", validConfigs.size());
        
        if (validConfigs.isEmpty()) {
            log.warn("Aucune table configurée ou activée");
            return;
        }
        
        log.info("Jobs disponibles: {}", tableJobs.keySet());
        log.info("Lancement de {} jobs", validConfigs.size());
        
        for (TableConfig config : validConfigs) {
            String jobName = config.getJobName();
            log.info("Recherche du job pour {} avec le nom: {}", config.getSourceTableName(), jobName);
            
            // Essayer le nom exact
            Job job = tableJobs.get(jobName);
            
            // Si non trouvé, essayer le job générique
            if (job == null && tableJobs.containsKey("trinoToPostgresJob")) {
                job = tableJobs.get("trinoToPostgresJob");
                log.info("Utilisation du job générique trinoToPostgresJob pour la table {}", 
                          config.getSourceTableName());
            }
            
            if (job != null) {
                log.info("Traitement: {} -> {}", 
                        config.getSourceTableName(), config.getTargetTableName());
                
                JobParameters params = createJobParameters(jobName, config);
                jobLauncher.run(job, params);
            } else {
                log.error("Job non trouvé pour: {}. Jobs disponibles: {}", 
                          config.getSourceTableName(), tableJobs.keySet());
            }
        }
    }
    
    /**
     * Crée les paramètres pour l'exécution d'un job
     */
    private JobParameters createJobParameters(String jobName, TableConfig config) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return new JobParametersBuilder()
                .addString("time", timestamp)
                .addString("jobName", jobName)
                .addString("sourceTableName", config.getSourceTableName())
                .addString("sourceSchema", config.getSourceSchema())
                .addString("targetTableName", config.getTargetTableName())
                .addString("targetSchema", config.getTargetSchema())
                .toJobParameters();
    }
} 