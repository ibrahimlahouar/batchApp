package com.ilahouar.batchapp.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Configuration des sources de données pour Trino et Oracle
 */
@Configuration
public class DataSourceConfig {

    /**
     * Configuration de la source de données Trino
     * Cette source sera utilisée pour lire les données
     */
    @Bean
    @Qualifier("trinoDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.trino")
    public DataSource trinoDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * Configuration de la source de données Oracle
     * Cette source sera utilisée pour écrire les données traitées
     */
    @Bean
    @Primary
    @Qualifier("oracleDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.oracle")
    public DataSource oracleDataSource() {
        return DataSourceBuilder.create().build();
    }
    
    /**
     * Bean dataSource par défaut pour Spring Batch
     * Réutilisation de la source Oracle pour les métadonnées de Spring Batch
     */
    @Bean(name = "dataSource")
    public DataSource dataSource(@Qualifier("oracleDataSource") DataSource oracleDataSource) {
        return oracleDataSource;
    }

    /**
     * Gestionnaire de transactions pour Oracle
     */
    @Bean
    public PlatformTransactionManager transactionManager(
            @Qualifier("oracleDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
} 