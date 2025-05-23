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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

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
    @Qualifier("postgresDataSource")
    private DataSource postgresDataSource;
    
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
        SchemaValidationProcessor processor = createProcessor(tableConfig, postgresDataSource);
        
        // Créer le writer spécifique à la table
        JdbcBatchItemWriter<Map<String, Object>> writer = createWriter(tableConfig, postgresDataSource);
        
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
    private SchemaValidationProcessor createProcessor(TableConfig tableConfig, DataSource postgresDataSource) {
        // Note: Ceci est une méthode simplifiée qui suppose que SchemaValidationProcessor
        // est adapté pour toutes les tables. Dans un cas réel, on pourrait avoir besoin
        // de créer des processors spécifiques à chaque table.
        
        // Ici on utilise une technique simple pour contourner le problème de création du bean
        // Dans une implémentation complète, on injecterait le SchemaValidationProcessor avec @Autowired
        return new SchemaValidationProcessor(postgresDataSource, tableConfig.getTargetTableName());
    }
    
    /**
     * Crée un writer pour la table spécifiée
     */
    private JdbcBatchItemWriter<Map<String, Object>> createWriter(TableConfig tableConfig, DataSource postgresDataSource) {
        String tableName = tableConfig.getTargetTableName();
        String schemaName = tableConfig.getTargetSchema();
        
        String fullTableName = tableName;
        if (schemaName != null && !schemaName.isEmpty()) {
            fullTableName = schemaName + "." + tableName;
        }
        
        log.info("Création d'un writer pour la table: {}", fullTableName);
        
        // Récupérer les colonnes de la table PostgreSQL
        JdbcTemplate jdbcTemplate = new JdbcTemplate(postgresDataSource);
        String query = "SELECT column_name, data_type FROM information_schema.columns " +
                     "WHERE table_name = ? ";
        
        if (schemaName != null && !schemaName.isEmpty()) {
            query += "AND table_schema = ? ";
        }
        
        query += "ORDER BY ordinal_position";
        
        List<Map<String, Object>> columnData;
        try {
            if (schemaName != null && !schemaName.isEmpty()) {
                columnData = jdbcTemplate.queryForList(query, tableName, schemaName);
            } else {
                columnData = jdbcTemplate.queryForList(query, tableName);
            }
            
            if (columnData.isEmpty()) {
                log.warn("Aucune colonne trouvée pour la table {}. Vérifiez que la table existe.", fullTableName);
                return createDefaultWriter(fullTableName, postgresDataSource);
            }
            
            // Identifier la clé primaire
            String pkQuery = "SELECT a.attname as column_name " +
                           "FROM pg_index i " +
                           "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                           "WHERE i.indrelid = ?::regclass AND i.indisprimary";
            
            String pkColumn = null;
            try {
                pkColumn = jdbcTemplate.queryForObject(pkQuery, String.class, fullTableName);
                log.info("Clé primaire identifiée pour la table {}: {}", fullTableName, pkColumn);
            } catch (Exception e) {
                log.warn("Impossible d'identifier la clé primaire pour {}: {}", fullTableName, e.getMessage());
            }
            
            // Extraire les noms de colonnes
            StringJoiner columnNames = new StringJoiner(", ");
            StringJoiner paramNames = new StringJoiner(", ");
            
            for (Map<String, Object> column : columnData) {
                String columnName = column.get("column_name").toString();
                String dataType = column.get("data_type").toString();
                
                columnNames.add(columnName);
                
                if ("jsonb".equals(dataType)) {
                    paramNames.add("cast(:" + columnName + " as jsonb)");
                } else {
                    paramNames.add(":" + columnName);
                }
            }
            
            String sql;
            if (pkColumn != null) {
                // Utiliser UPSERT (INSERT ON CONFLICT DO UPDATE) pour éviter les erreurs de clés dupliquées
                StringJoiner updateStatements = new StringJoiner(", ");
                for (Map<String, Object> column : columnData) {
                    String columnName = column.get("column_name").toString();
                    if (!columnName.equals(pkColumn)) {  // Ne pas inclure la clé primaire dans l'UPDATE
                        updateStatements.add(columnName + " = EXCLUDED." + columnName);
                    }
                }
                
                sql = String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", 
                        fullTableName, columnNames.toString(), paramNames.toString(), 
                        pkColumn, updateStatements.toString());
                log.info("Utilisation d'un UPSERT pour gérer les clés dupliquées: {}", sql);
            } else {
                // Si pas de clé primaire identifiée, on utilise INSERT simple
                sql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                        fullTableName, columnNames.toString(), paramNames.toString());
                log.warn("Clé primaire non identifiée, utilisation d'un INSERT simple: {}", sql);
            }
            
            JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
            writer.setDataSource(postgresDataSource);
            writer.setSql(sql);
            writer.setItemSqlParameterSourceProvider(item -> new MapSqlParameterSource(item));
            writer.afterPropertiesSet();
            
            return tableConfig.customizeWriter(writer);
        } catch (Exception e) {
            log.error("Erreur lors de la création du writer pour la table {}: {}", fullTableName, e.getMessage());
            return createDefaultWriter(fullTableName, postgresDataSource);
        }
    }
    
    /**
     * Crée un writer par défaut si la récupération des colonnes échoue
     */
    private JdbcBatchItemWriter<Map<String, Object>> createDefaultWriter(String tableName, DataSource postgresDataSource) {
        log.info("Création d'un writer par défaut pour la table: {}", tableName);
        
        // Essayer d'identifier la clé primaire
        JdbcTemplate jdbcTemplate = new JdbcTemplate(postgresDataSource);
        String pkQuery = "SELECT a.attname as column_name " +
                       "FROM pg_index i " +
                       "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                       "WHERE i.indrelid = ?::regclass AND i.indisprimary";
        
        try {
            final String pkColumn = jdbcTemplate.queryForObject(pkQuery, String.class, tableName);
            log.info("Clé primaire identifiée pour la table (mode par défaut): {}: {}", tableName, pkColumn);
            
            // Utiliser un UPSERT avec la clé primaire identifiée
            String sql = String.format("INSERT INTO %s (data, %s) VALUES (:data::jsonb, :%s) ON CONFLICT (%s) DO UPDATE SET data = :data::jsonb", 
                    tableName, pkColumn, pkColumn, pkColumn);
            
            JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
            writer.setDataSource(postgresDataSource);
            writer.setSql(sql);
            writer.setItemSqlParameterSourceProvider(item -> {
                try {
                    com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
                        new com.fasterxml.jackson.databind.ObjectMapper();
                    String jsonData = objectMapper.writeValueAsString(item);
                    
                    MapSqlParameterSource params = new MapSqlParameterSource("data", jsonData);
                    // Ajouter la clé primaire au paramètre
                    if (item.containsKey(pkColumn)) {
                        params.addValue(pkColumn, item.get(pkColumn));
                    }
                    return params;
                } catch (Exception e) {
                    log.error("Erreur de conversion JSON: {}", e.getMessage());
                    return new MapSqlParameterSource("data", "{}");
                }
            });
            writer.afterPropertiesSet();
            return writer;
            
        } catch (Exception e) {
            log.warn("Impossible d'identifier la clé primaire, utilisation d'un INSERT simple: {}", e.getMessage());
            
            // Utiliser un format de requête qui utilise un objet JSON sans UPSERT
            String sql = String.format("INSERT INTO %s (data) VALUES (:data::jsonb)", tableName);
            
            JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
            writer.setDataSource(postgresDataSource);
            writer.setSql(sql);
            writer.setItemSqlParameterSourceProvider(item -> {
                try {
                    // Convertir l'item en JSON
                    com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
                        new com.fasterxml.jackson.databind.ObjectMapper();
                    String jsonData = objectMapper.writeValueAsString(item);
                    return new MapSqlParameterSource("data", jsonData);
                } catch (Exception ex) {
                    log.error("Erreur de conversion JSON: {}", ex.getMessage());
                    return new MapSqlParameterSource("data", "{}");
                }
            });
            writer.afterPropertiesSet();
            return writer;
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