package com.ilahouar.batchapp.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Générateur de requêtes SQL dynamiques basé sur les options de configuration
 * Adapté pour la syntaxe Oracle
 */
@Component
@Slf4j
public class DynamicQueryBuilder {

    /**
     * Construit une requête SELECT dynamique en fonction des options, compatible avec Oracle
     */
    public String buildSelectQuery(String schema, String table, Map<String, Object> options) {
        StringBuilder query = new StringBuilder();
        
        // Gestion de LIMIT et OFFSET pour Oracle en utilisant ROWNUM ou ROW_NUMBER()
        boolean hasLimit = options.containsKey("limit") && options.get("limit") instanceof Number;
        boolean hasOrderBy = options.containsKey("orderBy");
        
        // Si on a un LIMIT, on doit utiliser une sous-requête avec Oracle
        if (hasLimit && !hasOrderBy) {
            query.append("SELECT * FROM (");
        } else if (hasLimit) {
            // Avec ORDER BY et LIMIT, utiliser une requête imbriquée avec ROW_NUMBER
            query.append("SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER(");
            
            // Ajouter l'ORDER BY dans la fonction OVER pour ROW_NUMBER
            if (options.get("orderBy") instanceof String) {
                query.append("ORDER BY ").append(options.get("orderBy"));
            } else if (options.get("orderBy") instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> orderByColumns = (List<String>) options.get("orderBy");
                if (!orderByColumns.isEmpty()) {
                    StringJoiner orderByJoiner = new StringJoiner(", ");
                    for (String column : orderByColumns) {
                        orderByJoiner.add(column);
                    }
                    query.append("ORDER BY ").append(orderByJoiner);
                }
            }
            
            query.append(") AS rn FROM ");
        } else {
            query.append("SELECT ");
        }
        
        // Continuer avec la clause SELECT standard si on n'a pas de LIMIT avec ORDER BY
        if (!hasLimit || !hasOrderBy) {
            // 1. Construire la clause SELECT
            if (options.containsKey("columns") && options.get("columns") instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> columns = (List<String>) options.get("columns");
                if (!columns.isEmpty()) {
                    StringJoiner columnJoiner = new StringJoiner(", ");
                    for (String column : columns) {
                        columnJoiner.add(column);
                    }
                    query.append(columnJoiner);
                } else {
                    query.append("*");
                }
            } else {
                query.append("*");
            }
            
            // 2. Construire la clause FROM
            query.append(" FROM ");
        }
        
        // Construction du nom de la table avec schéma si nécessaire
        if (schema != null && !schema.isEmpty()) {
            query.append(schema).append(".");
        }
        query.append(table);
        
        // Si on a une requête imbriquée avec ROW_NUMBER, on ferme la première partie
        if (hasLimit && hasOrderBy) {
            query.append(") t");
        }
        
        // 3. Construire la clause WHERE
        List<String> whereConditions = new ArrayList<>();
        
        if (options.containsKey("where") && options.get("where") instanceof String) {
            whereConditions.add(options.get("where").toString());
        }
        
        if (options.containsKey("filter") && options.get("filter") instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> filters = (Map<String, Object>) options.get("filter");
            for (Map.Entry<String, Object> filter : filters.entrySet()) {
                String column = filter.getKey();
                Object value = filter.getValue();
                
                if (value instanceof Number) {
                    whereConditions.add(column + " = " + value);
                } else if (value instanceof Boolean) {
                    // Oracle n'a pas de type BOOLEAN natif, utiliser 1/0
                    boolean boolValue = (Boolean) value;
                    whereConditions.add(column + " = " + (boolValue ? "1" : "0"));
                } else if (value instanceof String) {
                    whereConditions.add(column + " = '" + value + "'");
                } else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> valueList = (List<Object>) value;
                    if (!valueList.isEmpty()) {
                        StringJoiner valuesJoiner = new StringJoiner(", ", "(", ")");
                        for (Object val : valueList) {
                            if (val instanceof Number) {
                                valuesJoiner.add(val.toString());
                            } else if (val instanceof Boolean) {
                                // Oracle n'a pas de type BOOLEAN natif, utiliser 1/0
                                boolean boolVal = (Boolean) val;
                                valuesJoiner.add(boolVal ? "1" : "0");
                            } else {
                                valuesJoiner.add("'" + val + "'");
                            }
                        }
                        whereConditions.add(column + " IN " + valuesJoiner);
                    }
                }
            }
        }
        
        if (!whereConditions.isEmpty()) {
            query.append(" WHERE ");
            StringJoiner whereJoiner = new StringJoiner(" AND ");
            for (String condition : whereConditions) {
                whereJoiner.add(condition);
            }
            query.append(whereJoiner);
            
            // Ajouter la condition sur ROW_NUMBER pour LIMIT avec ORDER BY
            if (hasLimit && hasOrderBy) {
                query.append(" AND rn <= ").append(options.get("limit"));
            }
        } else if (hasLimit && hasOrderBy) {
            // Si pas de WHERE mais LIMIT avec ORDER BY
            query.append(" WHERE rn <= ").append(options.get("limit"));
        }
        
        // 4. Construire la clause ORDER BY standard (uniquement si pas combiné avec LIMIT)
        if (options.containsKey("orderBy") && !hasLimit) {
            if (options.get("orderBy") instanceof String) {
                query.append(" ORDER BY ").append(options.get("orderBy"));
            } else if (options.get("orderBy") instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> orderByColumns = (List<String>) options.get("orderBy");
                if (!orderByColumns.isEmpty()) {
                    StringJoiner orderByJoiner = new StringJoiner(", ");
                    for (String column : orderByColumns) {
                        orderByJoiner.add(column);
                    }
                    query.append(" ORDER BY ").append(orderByJoiner);
                }
            }
        }
        
        // 5. Ajouter ROWNUM pour LIMIT sans ORDER BY
        if (hasLimit && !hasOrderBy) {
            query.append(") WHERE ROWNUM <= ").append(options.get("limit"));
        }
        
        String finalQuery = query.toString();
        log.debug("Requête SQL Oracle générée: {}", finalQuery);
        return finalQuery;
    }
} 