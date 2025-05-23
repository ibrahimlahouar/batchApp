package com.ilahouar.batchapp.config;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Configuration principale du job Spring Batch
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class BatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TaskExecutor batchTaskExecutor;

    @Value("${batch.chunk-size}")
    private int chunkSize;

    @Value("${batch.throttle-limit}")
    private int throttleLimit;

    @Value("${batch.max-retries}")
    private int maxRetries;

    /**
     * Configuration du job principal
     */
    @Bean
    public Job trinoToPostgresJob(
            JobCompletionNotificationListener listener,
            @Qualifier("processDataStep") Step processDataStep) {
        return new JobBuilder("trinoToPostgresJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(processDataStep)
                .end()
                .build();
    }

    /**
     * Configuration de l'étape de traitement avec parallélisme et résilience
     */
    @Bean
    public Step processDataStep(
            @Qualifier("trinoItemReader") JdbcCursorItemReader<Map<String, Object>> reader,
            @Qualifier("postgresItemWriter") JdbcBatchItemWriter<Map<String, Object>> writer,
            SchemaValidationProcessor processor,
            StepSkipListener skipListener,
            SkipPolicy skipPolicy) {
        return new StepBuilder("processDataStep", jobRepository)
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
                .listener(skipListener)
                .stream(reader)
                .build();
    }

    /**
     * Configuration du reader pour Trino
     */
    @Bean
    @StepScope
    public JdbcCursorItemReader<Map<String, Object>> trinoItemReader(
            @Qualifier("trinoDataSource") DataSource dataSource,
            @Value("#{jobParameters['tableName'] ?: '${batch.trino.table-name:default_table}'}") String tableName) {
        
        log.info("Initialisation du reader pour la table Trino: {}", tableName);
        
        String sql = "SELECT * FROM " + tableName;
        log.info("Lecture depuis la table Trino: {}", tableName);
        
        return new JdbcCursorItemReaderBuilder<Map<String, Object>>()
                .name("trinoItemReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new ColumnMapRowMapper())
                .saveState(false)
                .build();
    }

    /**
     * Configuration du writer pour PostgreSQL
     */
    @Bean
    @StepScope
    public JdbcBatchItemWriter<Map<String, Object>> postgresItemWriter(
            @Qualifier("postgresDataSource") DataSource dataSource,
            @Value("#{jobParameters['targetTable'] ?: '${batch.postgres.table-name:default_table}'}") String tableName) {
        
        log.info("Initialisation du writer pour la table PostgreSQL: {}", tableName);
        
        // Récupérer les colonnes de la table PostgreSQL pour la requête d'insertion
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String query = "SELECT column_name, data_type FROM information_schema.columns " +
                     "WHERE table_name = ? ORDER BY ordinal_position";
        
        List<String> columns;
        List<String> dataTypes = new ArrayList<>();
        
        try {
            List<Map<String, Object>> columnData = jdbcTemplate.queryForList(query, tableName);
            columns = columnData.stream()
                    .map(col -> col.get("column_name").toString())
                    .collect(java.util.stream.Collectors.toList());
                    
            dataTypes = columnData.stream()
                    .map(col -> col.get("data_type").toString())
                    .collect(java.util.stream.Collectors.toList());
                    
            log.info("Colonnes identifiées pour la table {}: {}", tableName, columns);
            
            // Vérifier si la table a une structure générique avec JSONB
            boolean hasJsonbColumn = dataTypes.contains("jsonb");
            if (hasJsonbColumn) {
                log.info("Table {} utilise un format JSONB pour stocker les données", tableName);
            }
            
        } catch (Exception e) {
            log.warn("Impossible de récupérer les colonnes de la table {}, utilisation d'un INSERT générique. Erreur: {}", 
                     tableName, e.getMessage());
            // Requête d'insertion générique en cas d'échec
            return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                    .dataSource(dataSource)
                    .sql("INSERT INTO " + tableName + " VALUES ()")
                    .itemSqlParameterSourceProvider(item -> 
                        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource(item))
                    .build();
        }
        
        // Si aucune colonne trouvée, rien à insérer
        if (columns.isEmpty()) {
            log.warn("Aucune colonne trouvée dans la table {}, l'insertion échouera", tableName);
            // Requête d'insertion vide qui échouera, mais évite une erreur de syntaxe SQL
            return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                    .dataSource(dataSource)
                    .sql("INSERT INTO " + tableName + " DEFAULT VALUES")
                    .itemSqlParameterSourceProvider(item -> 
                        new org.springframework.jdbc.core.namedparam.MapSqlParameterSource(item))
                    .build();
        }
        
        // Vérifier si c'est une table avec auto-increment (SERIAL) pour l'ID
        boolean hasSerialId = false;
        for (int i = 0; i < columns.size(); i++) {
            if ("id".equals(columns.get(i))) {
                // Vérifier si c'est une séquence auto-générée
                try {
                    String seqQuery = "SELECT pg_get_serial_sequence(?, 'id') IS NOT NULL";
                    Boolean isSerial = jdbcTemplate.queryForObject(seqQuery, Boolean.class, tableName);
                    hasSerialId = isSerial != null && isSerial;
                    if (hasSerialId) {
                        log.info("Colonne 'id' détectée comme SERIAL/auto-increment");
                    }
                } catch (Exception e) {
                    log.debug("Erreur lors de la vérification de la séquence: {}", e.getMessage());
                }
                break;
            }
        }
        
        // Si une colonne JSONB est trouvée et que l'ID est auto-généré
        boolean hasJsonbColumn = dataTypes.contains("jsonb");
        if (hasJsonbColumn && hasSerialId) {
            // SQL pour insertion avec génération d'ID automatique
            String sql = "INSERT INTO " + tableName + " (data) VALUES (cast(:data as jsonb))";
            log.info("Requête d'insertion JSONB avec ID auto-généré: {}", sql);
            
            return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                    .dataSource(dataSource)
                    .sql(sql)
                    .itemSqlParameterSourceProvider(item -> {
                        try {
                            // Convertir l'objet en JSON pour PostgreSQL
                            com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
                                new com.fasterxml.jackson.databind.ObjectMapper();
                            String jsonData = objectMapper.writeValueAsString(item.get("data"));
                            
                            return new org.springframework.jdbc.core.namedparam.MapSqlParameterSource("data", jsonData);
                        } catch (Exception e) {
                            log.error("Erreur lors de la conversion en JSON: {}", e.getMessage());
                            return new org.springframework.jdbc.core.namedparam.MapSqlParameterSource("data", "{}");
                        }
                    })
                    .build();
        }
        
        // Construction de la requête d'insertion standard
        StringJoiner columnNames = new StringJoiner(", ");
        StringJoiner paramPlaceholders = new StringJoiner(", ");
        
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String dataType = dataTypes.get(i);
            
            columnNames.add(column);
            
            // Pour les colonnes JSONB, utiliser la conversion PostgreSQL
            if ("jsonb".equals(dataType)) {
                paramPlaceholders.add("cast(:" + column + " as jsonb)");
            } else {
                paramPlaceholders.add(":" + column);
            }
        }
        
        String insertSql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)", 
                tableName, 
                columnNames.toString(), 
                paramPlaceholders.toString());
        
        log.info("Requête d'insertion générique générée: {}", insertSql);
        
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(insertSql)
                .itemSqlParameterSourceProvider(item -> {
                    // Si l'item contient une clé 'data' qui est une Map, la convertir en JSON
                    if (item.containsKey("data") && item.get("data") instanceof Map) {
                        try {
                            org.springframework.jdbc.core.namedparam.MapSqlParameterSource parameterSource = 
                                new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
                            
                            // Convertir l'objet en JSON pour PostgreSQL
                            com.fasterxml.jackson.databind.ObjectMapper objectMapper = 
                                new com.fasterxml.jackson.databind.ObjectMapper();
                            String jsonData = objectMapper.writeValueAsString(item.get("data"));
                            
                            parameterSource.addValue("data", jsonData);
                            
                            // Ajouter l'ID s'il est présent
                            if (item.containsKey("id")) {
                                parameterSource.addValue("id", item.get("id"));
                            }
                            
                            return parameterSource;
                        } catch (Exception e) {
                            log.error("Erreur lors de la conversion en JSON: {}", e.getMessage());
                            // En cas d'erreur, retourner l'item tel quel
                            return new org.springframework.jdbc.core.namedparam.MapSqlParameterSource(item);
                        }
                    } else {
                        // Si ce n'est pas un format JSON, utiliser l'item tel quel
                        return new org.springframework.jdbc.core.namedparam.MapSqlParameterSource(item);
                    }
                })
                .build();
    }
} 