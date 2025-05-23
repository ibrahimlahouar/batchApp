package com.ilahouar.batchapp.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Générateur de requêtes SQL dynamiques basé sur les options de configuration
 */
@Component
@Slf4j
public class DynamicQueryBuilder {

    /**
     * Construit une requête SELECT dynamique en fonction des options
     */
    public String buildSelectQuery(String schema, String table, Map<String, Object> options) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        
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
        if (schema != null && !schema.isEmpty()) {
            query.append(schema).append(".");
        }
        query.append(table);
        
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
                    whereConditions.add(column + " = " + value);
                } else if (value instanceof String) {
                    whereConditions.add(column + " = '" + value + "'");
                } else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> valueList = (List<Object>) value;
                    if (!valueList.isEmpty()) {
                        StringJoiner valuesJoiner = new StringJoiner(", ", "(", ")");
                        for (Object val : valueList) {
                            if (val instanceof Number || val instanceof Boolean) {
                                valuesJoiner.add(val.toString());
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
        }
        
        // 4. Construire la clause ORDER BY
        if (options.containsKey("orderBy")) {
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
        
        // 5. Ajouter LIMIT si spécifié
        if (options.containsKey("limit") && options.get("limit") instanceof Number) {
            query.append(" LIMIT ").append(options.get("limit"));
        }
        
        String finalQuery = query.toString();
        log.debug("Requête SQL générée: {}", finalQuery);
        return finalQuery;
    }
} 