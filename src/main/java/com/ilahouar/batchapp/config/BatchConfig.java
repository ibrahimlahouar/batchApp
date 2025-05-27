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
    private final JobCompletionNotificationListener jobCompletionNotificationListener;
    private final StepSkipListener skipListener;

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
    public Job trinoToOracleJob(
            JobRepository jobRepository,
            Step trinoToOracleStep) {
        return new JobBuilder("trinoToOracleJob", jobRepository)
                .start(trinoToOracleStep)
                .listener(jobCompletionNotificationListener)
                .build();
    }

    /**
     * Configuration de l'étape de traitement avec parallélisme et résilience
     */
    @Bean
    public Step trinoToOracleStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("trinoItemReader") JdbcCursorItemReader<Map<String, Object>> reader,
            SchemaValidationProcessor processor,
            @Qualifier("oracleItemWriter") JdbcBatchItemWriter<Map<String, Object>> writer,
            SkipPolicy skipPolicy,
            @Value("${batch.chunk-size:5000}") int chunkSize) {
        return new StepBuilder("trinoToOracleStep", jobRepository)
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
     * Configuration du writer pour Oracle
     */
    @Bean
    @StepScope
    public JdbcBatchItemWriter<Map<String, Object>> oracleItemWriter(
            @Qualifier("oracleDataSource") DataSource dataSource,
            @Value("#{jobParameters['targetTable'] ?: '${batch.oracle.table-name:default_table}'}") String tableName) {
        
        log.info("Initialisation du writer pour la table Oracle: {}", tableName);
        
        // Récupérer les métadonnées de la table Oracle
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        
        try {
            // Récupérer les colonnes
            String query = "SELECT column_name FROM all_tab_columns WHERE table_name = ?";
            List<Map<String, Object>> columnMaps = jdbcTemplate.queryForList(query, tableName.toUpperCase());
            List<String> columns = columnMaps.stream()
                .map(col -> col.get("column_name").toString())
                .collect(java.util.stream.Collectors.toList());
            
            // Identifier les colonnes IDENTITY
            String identityQuery = "SELECT column_name FROM all_tab_identity_cols WHERE table_name = ?";
            List<Map<String, Object>> identityMaps = jdbcTemplate.queryForList(identityQuery, tableName.toUpperCase());
            List<String> identityColumns = identityMaps.stream()
                .map(col -> col.get("column_name").toString())
                .collect(java.util.stream.Collectors.toList());
            
            // Filtrer les colonnes IDENTITY
            columns.removeAll(identityColumns);
            
            if (columns.isEmpty()) {
                throw new RuntimeException("Aucune colonne non-IDENTITY trouvée pour " + tableName);
            }
            
            // Construire les parties de la requête
            StringJoiner columnNames = new StringJoiner(", ");
            StringJoiner paramNames = new StringJoiner(", ");
            
            for (String column : columns) {
                columnNames.add(column);
                // Préserver la casse originale
                paramNames.add(":" + column);
            }
            
            String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                    tableName, columnNames, paramNames);
            
            log.info("Writer standard pour {}: {}", tableName, insertSql);
            
            return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                    .dataSource(dataSource)
                    .sql(insertSql)
                    .itemSqlParameterSourceProvider(item -> {
                        org.springframework.jdbc.core.namedparam.MapSqlParameterSource paramSource = 
                            new org.springframework.jdbc.core.namedparam.MapSqlParameterSource();
                        
                        for (Map.Entry<String, Object> entry : item.entrySet()) {
                            // Préserver la casse originale
                            String key = entry.getKey();
                            Object value = entry.getValue();
                            
                            // Ne pas convertir les booléens
                            paramSource.addValue(key, value);
                        }
                        
                        return paramSource;
                    })
                    .build();
        } catch (Exception e) {
            log.error("Erreur lors de la création du writer pour {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Erreur lors de la création du writer pour " + tableName, e);
        }
    }
} 