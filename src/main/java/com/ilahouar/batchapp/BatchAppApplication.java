package com.ilahouar.batchapp;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Application principale pour le traitement batch de données entre Trino et PostgreSQL
 * 
 * Ce batch est conçu pour lire des données depuis une source Trino et les écrire dans PostgreSQL
 * avec un traitement parallèle pour gérer efficacement les grands volumes de données.
 */
@SpringBootApplication
@EnableBatchProcessing
public class BatchAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchAppApplication.class, args);
    }
} 