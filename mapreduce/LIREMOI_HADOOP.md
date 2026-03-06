# Lancer le classement des sources sur Hadoop (sans Python)

Le conteneur namenode n’a pas Python. On utilise **Hadoop Streaming** avec des scripts **shell** (awk). Tu peux tout faire en ligne de commande dans le namenode, puis **consulter les résultats via l’interface web Hadoop**.

---

## 1. Copier les scripts sur ta machine puis dans le conteneur

Sur ta machine (dossier du projet) :

```bash
cd /home/drelto/prog/bigdata-projet
docker cp mapreduce/mapper.sh namenode:/root/
docker cp mapreduce/reducer.sh namenode:/root/
```

Rendre les scripts exécutables (dans le conteneur) :

```bash
docker exec -it namenode bash -c "chmod +x /root/mapper.sh /root/reducer.sh"
```

---

## 2. Lancer le job MapReduce depuis le namenode

Entre dans le conteneur :

```bash
docker exec -it namenode bash
```

Puis exécute (en remplaçant **CHEMIN_ENTREE** par ton chemin HDFS, par ex. `/user/airflow/gdelt`) :

```bash
cd /root

# Trouver le JAR streaming (souvent ici)
export JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -1)

# Lancer le job (remplacer CHEMIN_ENTREE par ton dossier HDFS contenant les données GDELT)
hadoop jar $JAR \
  -input CHEMIN_ENTREE \
  -output /user/gdelt/classement_sources \
  -mapper "sh mapper.sh" \
  -reducer "sh reducer.sh" \
  -file mapper.sh \
  -file reducer.sh
```

Exemple si tes données sont dans `/user/airflow/gdelt` :

```bash
hadoop jar $JAR \
  -input /user/airflow/gdelt \
  -output /user/gdelt/classement_sources \
  -mapper "sh mapper.sh" \
  -reducer "sh reducer.sh" \
  -file mapper.sh \
  -file reducer.sh
```

Le job tourne sur le cluster (YARN). À la fin, la sortie est dans **`/user/gdelt/classement_sources`** sur HDFS.

---

## 3. Regarder via l’interface Hadoop

1. **Ouvrir l’interface HDFS** (NameNode) :  
   `http://172.20.0.190:9870` (ou l’IP de ta machine si c’est différent).

2. **Utilities** → **Browse the file system** (ou “Browse Directory”).

3. Aller dans **`/user`** → **`gdelt`** → **`classement_sources`**.

4. Ouvrir les fichiers **`part-00000`**, **`part-00001`**, etc. : chaque ligne est de la forme **`source`** `tab` **`nombre`** (nombre d’articles pour cette source).

Tu peux aussi voir la **liste des applications** (ton job) dans l’interface **YARN ResourceManager** (souvent sur le port **8088**), si elle est exposée.

---

## 4. Voir le résultat en ligne de commande (optionnel)

Dans le conteneur namenode :

```bash
hadoop fs -cat /user/gdelt/classement_sources/part-* | sort -t$'\t' -k2 -nr | head -20
```

Cela affiche le top 20 des sources par nombre d’articles.

---

## Résumé

- **Pas de Python** : mapper et reducer sont des scripts shell (`mapper.sh`, `reducer.sh`) avec **awk**.
- **Lancement** : dans le conteneur namenode, avec `hadoop jar ... -mapper "sh mapper.sh" -reducer "sh reducer.sh"`.
- **Résultats** : dans HDFS sous `/user/gdelt/classement_sources/`, à consulter via l’**interface web** (Browse the file system) comme demandé par ton prof.
