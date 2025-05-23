package com.ilahouar.batchapp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Configuration du ThreadPoolTaskExecutor pour le traitement parallèle
 */
@Configuration
public class TaskExecutorConfig {

    @Value("${batch.thread-pool.core-size}")
    private int corePoolSize;

    @Value("${batch.thread-pool.max-size}")
    private int maxPoolSize;

    @Value("${batch.thread-pool.queue-capacity}")
    private int queueCapacity;

    /**
     * Configure l'exécuteur de tâches qui gère les threads parallèles
     * Le nombre de threads est paramétré dans le fichier application.yml
     */
    @Bean
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("batch-thread-");
        // Politique de rejet - lors d'une surcharge, le thread appelant exécute la tâche
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }
} 