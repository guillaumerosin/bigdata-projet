# Big Data GDELT Data Pipeline

![Big Data](https://img.shields.io/badge/BigData-Pipeline-blue)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-black)
![Airflow](https://img.shields.io/badge/Workflow-Airflow-red)
![Elasticsearch](https://img.shields.io/badge/Search-Elasticsearch-yellow)
![HDFS](https://img.shields.io/badge/DataLake-Hadoop-green)
![Docker](https://img.shields.io/badge/Infrastructure-Docker-blue)

## Project Overview

This project implements a **distributed Big Data pipeline** designed to ingest, process and store large volumes of real-time news event data from the **GDELT dataset**.

The pipeline demonstrates how modern **data engineering architectures** handle streaming data, distributed storage and search indexing using industry technologies.

The project was developed as part of a **Big Data academic project**.

---

# Dataset

The project uses the **GDELT (Global Database of Events, Language and Tone)** dataset.

GDELT continuously monitors global news sources and extracts structured information about events happening around the world.

Key characteristics:

- Global news monitoring
- Updated every **15 minutes**
- Massive historical archive
- Event-based structured dataset

Dataset source: http://data.gdeltproject.org/gdeltv2/
More information: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/


---

# System Architecture

The project implements a **data pipeline composed of multiple distributed services**.
GDELT Dataset
│
▼
Data Extraction
│
▼
Apache Kafka (Streaming)
│
├──────────────► Hadoop HDFS (Data Lake)
│
├──────────────► ScyllaDB (Wide Column Store)
│
▼
Elasticsearch (Document Store)
│
▼
Kibana Dashboard (Data Visualization)


Workflow orchestration is handled by **Apache Airflow**.

---

# Technologies Used

### Data Streaming

- Apache Kafka

Kafka is used as the **central data streaming backbone** of the pipeline.

Each event is published as a **Key-Value message** in a Kafka topic.

---

### Workflow Orchestration

- Apache Airflow

Airflow manages the pipeline using **DAGs (Directed Acyclic Graphs)** to orchestrate the different tasks.

Tasks can run:

- sequentially
- in parallel

---

### Data Lake

- Hadoop HDFS

HDFS stores **raw GDELT messages** in the Data Lake.

The raw format is preserved to allow future processing and large-scale analysis.

---

### Wide Column Store

- ScyllaDB

ScyllaDB stores event data using a **Column-Family data model**.

It provides:

- high performance
- horizontal scalability
- compatibility with Cassandra APIs

---

### Document Store

- Elasticsearch

Elasticsearch indexes transformed event messages as **JSON documents**, allowing:

- fast search
- analytics queries
- real-time exploration

---

### Data Visualization

- Kibana

Kibana provides dashboards to explore the data indexed in Elasticsearch.

Example analyses:

- number of events per country
- distribution of event types
- most active news sources

---

### Infrastructure

- Docker
- Docker Compose

All services run in **isolated containers** forming a distributed environment.

Each container represents a **single service** in the architecture.

---

# Data Pipeline Phases

## Phase 1 – Data Extraction

Tasks:

- Extract GDELT CSV files
- Parse event records
- Send messages to a **Kafka topic**

Each message follows a **Key-Value structure**.

---

## Phase 2 – Data Lake Storage

Messages are consumed from Kafka and stored in **Hadoop HDFS**.

Characteristics:

- Raw storage
- No transformation
- Large-scale distributed storage

---

## Phase 3 – Wide Column Storage

Messages are transformed and inserted into **ScyllaDB**.

Data is structured according to the **Wide Column Store paradigm**.

---

## Phase 4 – Document Indexing

Messages are transformed into **JSON documents** and indexed into **Elasticsearch**.

This allows efficient search and analysis.

---

## Phase 5 – Data Visualization

Using **Kibana dashboards**, users can explore and analyze the stored events.

---

# Infrastructure Overview

The platform runs several services:

| Service | Role |
|------|------|
| Kafka | Streaming message broker |
| Airflow | Workflow orchestration |
| Hadoop | Data Lake storage |
| ScyllaDB | Wide column database |
| Elasticsearch | Document indexing |
| Kibana | Data visualization |

Each service runs inside a **Docker container**.




