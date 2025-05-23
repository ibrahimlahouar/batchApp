# Application de traitement Big Data Trino vers PostgreSQL

Cette application Spring Boot utilise Spring Batch pour traiter efficacement des milliards de lignes de données depuis une source Trino vers une base de données PostgreSQL.

## Fonctionnalités

- **Lecture efficace** depuis Trino via JDBC
- **Écriture optimisée** vers PostgreSQL
- **Traitement parallèle** avec ThreadPoolTaskExecutor
- **Résilience** avec mécanisme de retry et skip
- **Validation des schémas** entre la source et la destination
- **Logging avancé** des erreurs et des événements
- **Paramétrage flexible** via le fichier application.yml

## Prérequis

- Java 17+
- Maven 3.6+
- Une base de données Trino (source des données)
- Une base de données PostgreSQL (destination des données)

## Configuration

Avant de lancer l'application, modifiez le fichier `src/main/resources/application.yml` avec :

1. Les informations de connexion à votre instance Trino
2. Les informations de connexion à votre base PostgreSQL
3. Le paramétrage du traitement parallèle (threads, chunk size, etc.)

## Compilation

```bash
mvn clean package
```

## Utilisation

L'application nécessite le paramètre `tableName` pour spécifier la table source dans Trino :

```bash
java -jar target/batchApp-0.0.1-SNAPSHOT.jar --tableName=source_table
```

### Paramètres obligatoires

- `--tableName` : Nom de la table à lire dans Trino

### Utilisation avec différents projets

Cette application peut être utilisée avec différents projets (P1, P2, P3), chacun ayant des besoins spécifiques :

1. **Configurer la table cible** dans `application.yml` avec le paramètre `batch.postgres.table-name`
2. **Lancer l'application** en spécifiant la table source correspondant au projet

## Architecture

L'application est structurée en plusieurs composants :

- **Configuration des sources de données** : Connexion à Trino et PostgreSQL
- **Configuration du Batch** : Définition du job, des steps et du parallélisme
- **Processeur de validation de schéma** : Vérification de la compatibilité des champs
- **Listeners** : Suivi du job et gestion des erreurs
- **Launcher** : Démarrage du job avec les paramètres

## Performances et optimisation

L'application est optimisée pour traiter de grandes quantités de données grâce à :

- Le traitement par chunks configurable
- Le parallélisme avec ThreadPoolTaskExecutor
- La limitation du throttling pour éviter la surcharge
- La gestion des connexions JDBC avec pooling 