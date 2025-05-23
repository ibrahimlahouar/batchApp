package com.ilahouar.batchapp.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Listener pour suivre le début et la fin du job batch
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JobCompletionNotificationListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("==== DÉMARRAGE DU JOB: {} ====", jobExecution.getJobInstance().getJobName());
        log.info("Paramètres: {}", jobExecution.getJobParameters());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("==== JOB TERMINÉ AVEC SUCCÈS: {} ====", 
                    jobExecution.getJobInstance().getJobName());
            log.info("Heure de fin: {}", jobExecution.getEndTime());
            log.info("Total lignes traitées: {}", 
                    jobExecution.getStepExecutions().stream()
                            .mapToLong(step -> step.getWriteCount())
                            .sum());
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("==== JOB EN ÉCHEC: {} ====", jobExecution.getJobInstance().getJobName());
            log.error("Heure d'échec: {}", jobExecution.getEndTime());
            log.error("Statut de sortie: {}", jobExecution.getExitStatus().getExitDescription());
            
            jobExecution.getStepExecutions().forEach(stepExecution -> {
                log.error("Étape: {} - Statut: {}", 
                        stepExecution.getStepName(), stepExecution.getStatus());
                log.error("Erreurs: {} / Lectures: {} / Écritures: {} / Filtres: {}", 
                        stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount(),
                        stepExecution.getReadCount(),
                        stepExecution.getWriteCount(),
                        stepExecution.getFilterCount());
            });
        }
    }
} 