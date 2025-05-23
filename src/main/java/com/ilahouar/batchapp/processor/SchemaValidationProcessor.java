package com.ilahouar.batchapp.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processeur qui vérifie la compatibilité entre les schémas de la source Trino et de la cible PostgreSQL
 * et qui effectue les transformations nécessaires entre les deux
 */
@Component
@StepScope
@Slf4j
public class SchemaValidationProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final JdbcTemplate postgresTemplate;
    private final String targetTable;
    private final Map<String, List<Map<String, Object>>> tableColumnsCache = new ConcurrentHashMap<>();
    
    public SchemaValidationProcessor(
            @Qualifier("postgresDataSource") DataSource postgresDataSource,
            @Value("#{jobParameters['targetTable'] ?: '${batch.postgres.table-name:default_table}'}") String targetTable) {
        this.postgresTemplate = new JdbcTemplate(postgresDataSource);
        this.targetTable = targetTable;
        log.info("SchemaValidationProcessor initialisé avec la table cible: {}", targetTable);
        // Initialisation du cache à la création pour éviter les requêtes multiples
        initializeColumnsCache();
    }
    
    /**
     * Initialise le cache des colonnes de la table cible
     */
    private void initializeColumnsCache() {
        try {
            String query = "SELECT column_name, data_type FROM information_schema.columns " +
                         "WHERE table_name = ? ORDER BY ordinal_position";
            List<Map<String, Object>> columns = postgresTemplate.queryForList(query, targetTable);
            tableColumnsCache.put(targetTable, columns);
            log.info("Schéma de la table cible {} chargé: {} colonnes", targetTable, columns.size());
            
            // Si la table existe mais n'a pas de colonnes, ou si elle n'existe pas, il faut la créer
            if (columns.isEmpty()) {
                createGenericTable();
            }
        } catch (Exception e) {
            log.warn("Erreur lors du chargement du schéma de la table {}: {}", targetTable, e.getMessage());
            createGenericTable();
        }
    }
    
    /**
     * Crée une table générique avec une colonne data de type JSONB pour stocker les données
     */
    private void createGenericTable() {
        try {
            log.info("Création d'une table générique {} car le schéma est vide", targetTable);
            
            // Vérifier si la table existe
            try {
                postgresTemplate.queryForObject(
                        "SELECT to_regclass(?) IS NOT NULL",
                        Boolean.class,
                        targetTable);
                
                // Si la table existe mais est vide, on la supprime pour la recréer avec un schéma générique
                postgresTemplate.execute("DROP TABLE IF EXISTS " + targetTable);
                log.info("Table existante {} supprimée pour recréation", targetTable);
            } catch (Exception e) {
                log.info("La table {} n'existe pas encore, va être créée", targetTable);
            }
            
            // Créer une table générique
            postgresTemplate.execute("CREATE TABLE " + targetTable + " (" +
                    "id SERIAL PRIMARY KEY, " +
                    "data JSONB" +
                    ")");
            
            // Mettre à jour le cache avec la nouvelle structure
            List<Map<String, Object>> columns = new ArrayList<>();
            Map<String, Object> idColumn = new HashMap<>();
            idColumn.put("column_name", "id");
            idColumn.put("data_type", "integer");
            columns.add(idColumn);
            
            Map<String, Object> dataColumn = new HashMap<>();
            dataColumn.put("column_name", "data");
            dataColumn.put("data_type", "jsonb");
            columns.add(dataColumn);
            
            tableColumnsCache.put(targetTable, columns);
            
            log.info("Table générique {} créée avec succès avec une colonne JSONB", targetTable);
        } catch (Exception e) {
            log.error("Erreur lors de la création de la table générique {}: {}", targetTable, e.getMessage());
            throw new RuntimeException("Impossible de créer la table cible", e);
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> sourceItem) throws Exception {
        if (sourceItem == null) {
            return null;
        }
        
        List<Map<String, Object>> targetColumns = tableColumnsCache.get(targetTable);
        if (targetColumns == null || targetColumns.isEmpty()) {
            // Si on arrive ici, c'est que la table n'a pas pu être créée correctement
            throw new IllegalStateException("Impossible de trouver le schéma de la table cible: " + targetTable);
        }
        
        // Si le schéma inclut une colonne 'data' de type JSONB, on y stocke l'objet entier
        boolean hasJsonbColumn = targetColumns.stream()
                .anyMatch(column -> "data".equals(column.get("column_name")) && 
                                   "jsonb".equals(column.get("data_type")));
        
        if (hasJsonbColumn) {
            Map<String, Object> transformedItem = new HashMap<>();
            
            // Pour le mode JSONB, ne pas inclure l'ID dans le contenu JSON
            // L'ID sera généré automatiquement par PostgreSQL avec SERIAL
            transformedItem.put("data", sourceItem);
            
            // Dans le cas d'une table avec JSONB, on n'inclut pas le champ id pour laisser
            // PostgreSQL générer l'ID avec SERIAL/auto-increment
            return transformedItem;
        }
        
        // Sinon, on fait la transformation standard colonne par colonne
        Map<String, Object> transformedItem = new HashMap<>();
        
        // Vérification et transformation des champs
        for (Map<String, Object> column : targetColumns) {
            String columnName = column.get("column_name").toString();
            String dataType = column.get("data_type").toString();
            
            // Conversion des noms de colonnes (Trino peut avoir des majuscules, PostgreSQL généralement en minuscules)
            Object value = findValueInSourceItem(sourceItem, columnName);
            
            // Si la valeur existe dans la source, l'ajouter à l'élément transformé
            if (value != null) {
                // Conversion de type si nécessaire
                value = convertValueToTargetType(value, dataType);
                transformedItem.put(columnName, value);
            }
        }
        
        return transformedItem;
    }
    
    /**
     * Recherche une valeur dans l'élément source en tenant compte des variations de casse
     */
    private Object findValueInSourceItem(Map<String, Object> sourceItem, String columnName) {
        // Vérifier la correspondance exacte
        if (sourceItem.containsKey(columnName)) {
            return sourceItem.get(columnName);
        }
        
        // Vérifier la correspondance insensible à la casse
        for (Map.Entry<String, Object> entry : sourceItem.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }
        
        return null;
    }
    
    /**
     * Convertit la valeur au type cible requis par PostgreSQL
     */
    private Object convertValueToTargetType(Object value, String targetDataType) {
        if (value == null) {
            return null;
        }
        
        // Conversions spécifiques selon le type cible
        try {
            switch (targetDataType.toLowerCase()) {
                case "integer":
                case "int":
                case "int4":
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else if (value instanceof String) {
                        return Integer.parseInt((String) value);
                    }
                    break;
                case "bigint":
                case "int8":
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    } else if (value instanceof String) {
                        return Long.parseLong((String) value);
                    }
                    break;
                case "numeric":
                case "decimal":
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else if (value instanceof String) {
                        return Double.parseDouble((String) value);
                    }
                    break;
                case "boolean":
                case "bool":
                    if (value instanceof Boolean) {
                        return value;
                    } else if (value instanceof String) {
                        return Boolean.parseBoolean((String) value);
                    } else if (value instanceof Number) {
                        return ((Number) value).intValue() != 0;
                    }
                    break;
                // Par défaut, renvoyer la valeur telle quelle
                default:
                    return value;
            }
        } catch (Exception e) {
            log.warn("Erreur lors de la conversion de la valeur '{}' au type {}: {}", 
                    value, targetDataType, e.getMessage());
        }
        
        // En cas d'échec de conversion, renvoyer la valeur originale
        return value;
    }
} 