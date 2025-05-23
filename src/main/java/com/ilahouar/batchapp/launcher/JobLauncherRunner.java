package com.ilahouar.batchapp.launcher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * Lanceur de job qui démarre le batch au lancement de l'application
 * avec les paramètres fournis en ligne de commande
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobLauncherRunner implements ApplicationRunner {

    private final JobLauncher jobLauncher;
    private final Job trinoToPostgresJob;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String tableName = null;
        
        // Récupération du paramètre tableName depuis les arguments
        if (args.containsOption("tableName")) {
            tableName = args.getOptionValues("tableName").get(0);
        } else {
            log.error("Paramètre tableName manquant. Utilisez --tableName=<nom_table>");
            return;
        }
        
        log.info("Démarrage du job avec la table source et cible: {}", tableName);
        
        // Construction des paramètres du job
        JobParameters params = new JobParametersBuilder()
                .addString("tableName", tableName)
                .addString("targetTable", tableName)
                .addDate("runDate", new Date())
                .toJobParameters();
        
        // Lancement du job
        jobLauncher.run(trinoToPostgresJob, params);
    }
} 