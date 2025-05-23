# Batch App - Transfert de données Trino vers Oracle

Cette application Spring Batch permet de transférer des données de tables Trino vers Oracle de manière efficace et configurable.

## Caractéristiques

- Traitement parallèle avec chunks pour améliorer les performances
- Support de centaines de tables et schémas via configuration YAML
- Définition des tables sans code Java
- Configuration par profil pour différents projets (P1, P2, P3)
- Transformation et validation des données entre les sources
- Gestion flexible des schémas (création automatique au besoin)
- Support des données JSON pour une flexibilité maximale
- Gestion d'erreurs avec retry, skip et logging
- Monitoring et suivi des jobs

## Utilisation

### Lancement avec un profil spécifique

Pour lancer l'application avec le profil P1 :

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=p1 -Dspring-boot.run.arguments="--tableName=orders"
```

Pour lancer l'application avec le profil P2 :

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=p2 -Dspring-boot.run.arguments="--tableName=products"
```

Pour lancer l'application avec le profil P3 :

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=p3 -Dspring-boot.run.arguments="--tableName=customers"
```

### Paramètres

- `--tableName`: Nom de la table source dans Trino
- `--targetTable`: Nom de la table cible dans Oracle (par défaut, identique à tableName)
- `--jobName`: Nom du job à exécuter (optionnel, alternative à tableName)

## Profils disponibles

- **p1**: Configuration pour le projet P1 (tables TPCH)
- **p2**: Configuration pour le projet P2 (tables E-commerce)
- **p3**: Configuration pour le projet P3 (tables Analytics)

## Configuration des tables dans YAML

Toute la configuration des tables se fait dans les fichiers YAML. Pour ajouter des tables, 
modifiez simplement le fichier de configuration du profil correspondant :

- `application-p1.yml` pour le projet P1
- `application-p2.yml` pour le projet P2
- `application-p3.yml` pour le projet P3

### Format de configuration

```yaml
batch:
  tables:
    nom_de_table:
      source-schema: schema_source
      source-table: nom_table_source  # Optionnel, utilise la clé par défaut
      target-schema: schema_cible
      target-table: nom_table_cible   # Optionnel, utilise la clé par défaut
      options:
        # Options avancées (toutes optionnelles)
        columns: [col1, col2, col3]    # Colonnes à sélectionner
        where: "condition SQL"         # Condition WHERE 
        orderBy: "colonne ASC/DESC"    # Ordre de tri
        limit: 10000                   # Nombre max de lignes
        fetchSize: 5000                # Taille de lecture JDBC
        customQuery: "SELECT ..."      # Requête SQL personnalisée
```

## Architecture

L'application utilise une architecture standardisée :

- **DynamicTableConfig**: Configuration générique pour toutes les tables
- **DynamicTableConfigLoader**: Charge les configurations depuis les fichiers YAML
- **TableJobFactory**: Crée les jobs à partir des configurations
- **MultiTableJobLauncher**: Lance les jobs en fonction des paramètres

## Ajout de nouvelles tables

Il suffit de modifier le fichier YAML du profil correspondant pour ajouter une nouvelle table.
L'application détectera automatiquement cette table et créera le job nécessaire.

## Configuration

Voir les fichiers `application-*.yml` pour la configuration détaillée de chaque profil.

## Dépendances

- Spring Boot 3.1.5
- Spring Batch
- Oracle JDBC Driver
- Trino JDBC Driver
- Jackson pour le traitement JSON 