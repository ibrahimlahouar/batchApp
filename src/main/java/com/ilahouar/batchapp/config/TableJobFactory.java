package com.ilahouar.batchapp.config;

import com.ilahouar.batchapp.config.table.TableConfig;
import com.ilahouar.batchapp.listener.JobCompletionNotificationListener;
import com.ilahouar.batchapp.listener.StepSkipListener;
import com.ilahouar.batchapp.processor.SchemaValidationProcessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import jakarta.annotation.Resource;

/**
 * Factory pour créer des jobs basés sur différentes configurations de tables
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class TableJobFactory {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TaskExecutor batchTaskExecutor;
    private final JobCompletionNotificationListener jobCompletionListener;
    private final StepSkipListener stepSkipListener;
    private final SkipPolicy skipPolicy;
    
    @Autowired
    @Qualifier("trinoDataSource")
    private DataSource trinoDataSource;
    
    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource oracleDataSource;
    
    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.throttle-limit}")
    private int throttleLimit;

    @Value("${batch.max-retries}")
    private int maxRetries;
    
    /**
     * Crée un job pour une configuration de table spécifique
     */
    public Job createJobForTable(TableConfig tableConfig) {
        
        if (!tableConfig.isEnabled()) {
            log.info("Le job pour la table {} est désactivé", tableConfig.getSourceTableName());
            return null;
        }
        
        String jobName = tableConfig.getJobName();
        log.info("Création du job {} pour la table source {}", jobName, tableConfig.getSourceTableName());
        
        // Créer le reader spécifique à la table
        JdbcCursorItemReader<Map<String, Object>> reader = createReader(tableConfig, trinoDataSource);
        
        // Créer le processor (utilise SchemaValidationProcessor existant)
        SchemaValidationProcessor processor = createProcessor(tableConfig, oracleDataSource);
        
        // Créer le writer spécifique à la table
        JdbcBatchItemWriter<Map<String, Object>> writer = createWriter(tableConfig, oracleDataSource);
        
        // Créer l'étape
        Step step = createStep(jobName + "_step", reader, processor, writer);
        
        // Créer et retourner le job
        return new JobBuilder(jobName, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionListener)
                .start(step)
                .build();
    }
    
    /**
     * Crée un reader pour la table spécifiée
     */
    private JdbcCursorItemReader<Map<String, Object>> createReader(TableConfig tableConfig, DataSource trinoDataSource) {
        String query = tableConfig.createSourceQuery();
        log.info("Création d'un reader pour la requête: {}", query);
        
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReaderBuilder<Map<String, Object>>()
                .name(tableConfig.getSourceTableName() + "Reader")
                .dataSource(trinoDataSource)
                .sql(query)
                .rowMapper(new ColumnMapRowMapper())
                .saveState(false)
                .build();
        
        // Appliquer les personnalisations spécifiques à la table
        return tableConfig.customizeReader(reader);
    }
    
    /**
     * Crée un processor pour la table spécifiée
     */
    private SchemaValidationProcessor createProcessor(TableConfig tableConfig, DataSource oracleDataSource) {
        log.info("Création du processeur pour la table {}", tableConfig.getTargetTableName());
        
        // Dans cet exemple, on crée directement l'instance sans personnalisation spécifique par table
        return new SchemaValidationProcessor(oracleDataSource, tableConfig.getTargetTableName());
    }
    
    /**
     * Crée un writer pour la table spécifiée
     */
    private JdbcBatchItemWriter<Map<String, Object>> createWriter(TableConfig tableConfig, DataSource oracleDataSource) {
        String tableName = tableConfig.getTargetTableName();
        String schemaName = tableConfig.getTargetSchema();
        
        String fullTableName = tableName;
        if (schemaName != null && !schemaName.isEmpty()) {
            fullTableName = schemaName + "." + tableName;
        }
        
        log.info("Création d'un writer pour la table: {}", fullTableName);
        
        // Récupérer les colonnes de la table Oracle
        JdbcTemplate jdbcTemplate = new JdbcTemplate(oracleDataSource);
        String query = "SELECT column_name, data_type FROM all_tab_columns WHERE table_name = ?";
        
        if (schemaName != null && !schemaName.isEmpty()) {
            query += " AND owner = ?";
        }
        
        try {
            // Récupérer toutes les colonnes sauf les IDENTITY
            List<Map<String, Object>> columnData;
            if (schemaName != null && !schemaName.isEmpty()) {
                columnData = jdbcTemplate.queryForList(query, tableName.toUpperCase(), schemaName.toUpperCase());
            } else {
                columnData = jdbcTemplate.queryForList(query, tableName.toUpperCase());
            }
            
            // Identifier les colonnes IDENTITY à exclure
            String identityQuery = "SELECT column_name FROM all_tab_identity_cols WHERE table_name = ?";
            List<String> identityColumns = new ArrayList<>();
            try {
                List<Map<String, Object>> identityData = jdbcTemplate.queryForList(
                        identityQuery, 
                        tableName.toUpperCase());
                
                identityColumns = identityData.stream()
                    .map(col -> col.get("column_name").toString())
                    .collect(java.util.stream.Collectors.toList());
            } catch (Exception e) {
                log.debug("Erreur lors de la récupération des colonnes IDENTITY: {}", e.getMessage());
            }
            
            // Construire les listes de colonnes et paramètres
            StringJoiner columnNames = new StringJoiner(", ");
            StringJoiner paramNames = new StringJoiner(", ");
            
            for (Map<String, Object> column : columnData) {
                String columnName = column.get("column_name").toString();
                
                // Exclure les colonnes IDENTITY
                if (identityColumns.contains(columnName)) {
                    continue;
                }
                
                columnNames.add(columnName);
                // Préserver la casse originale des noms de paramètres
                paramNames.add(":" + columnName);
            }
            
            // S'il n'y a pas de colonnes, utiliser un writer générique
            if (columnNames.toString().isEmpty()) {
                log.warn("Aucune colonne exploitable trouvée pour {}.", fullTableName);
                throw new RuntimeException("Aucune colonne exploitable trouvée pour " + fullTableName);
            }
            
            // Créer la requête INSERT
            String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                    fullTableName, columnNames, paramNames);
            
            log.info("Writer standard pour {}: {}", fullTableName, insertSql);
            
            return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                    .dataSource(oracleDataSource)
                    .sql(insertSql)
                    .itemSqlParameterSourceProvider(item -> {
                        MapSqlParameterSource params = new MapSqlParameterSource();
                        
                        for (Map.Entry<String, Object> entry : item.entrySet()) {
                            // Préserver la casse originale des clés
                            String key = entry.getKey();
                            Object value = entry.getValue();
                            
                            // Ne pas convertir les booléens
                            params.addValue(key, value);
                        }
                        
                        return params;
                    })
                    .build();
        } catch (Exception e) {
            log.error("Erreur lors de la création du writer pour {}: {}", fullTableName, e.getMessage());
            throw new RuntimeException("Erreur lors de la création du writer pour " + fullTableName, e);
        }
    }
    
    /**
     * Crée une étape de traitement
     */
    private Step createStep(String stepName, 
                         JdbcCursorItemReader<Map<String, Object>> reader,
                         SchemaValidationProcessor processor,
                         JdbcBatchItemWriter<Map<String, Object>> writer) {
        
        return new StepBuilder(stepName, jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .taskExecutor(batchTaskExecutor)
                .throttleLimit(throttleLimit)
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(maxRetries)
                .skipPolicy(skipPolicy)
                .listener(stepSkipListener)
                .build();
    }
} 