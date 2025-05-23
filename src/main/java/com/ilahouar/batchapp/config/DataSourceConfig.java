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
 * Configuration des sources de données pour Trino et PostgreSQL
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
     * Configuration de la source de données PostgreSQL
     * Cette source sera utilisée pour écrire les données traitées
     */
    @Bean
    @Primary
    @Qualifier("postgresDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.postgres")
    public DataSource postgresDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * Gestionnaire de transactions pour PostgreSQL
     */
    @Bean
    public PlatformTransactionManager transactionManager(
            @Qualifier("postgresDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
} 