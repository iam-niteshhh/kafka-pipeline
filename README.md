# Real-Time Kafka Streaming Pipeline

## Overview
This project demonstrates a real-time data streaming pipeline using Kafka, Python, and PostgreSQL. It simulates user event ingestion, processes data in real-time, and stores it in a relational database for analytics.

---

## Architecture
Producer → Kafka → Consumer Group → Batch Processor → PostgreSQL


---

## Tech Stack
- Python  
- Apache Kafka  
- PostgreSQL  
- Docker  
- Kafka-Python  

---

## Features
- Real-time event streaming using Kafka  
- Consumer group-based parallel processing  
- Batch inserts for optimized database performance  
- Scalable partition-based architecture  
- Dockerized setup for easy deployment  

---

## Data Flow
1. Python producer generates user events  
2. Kafka ingests and buffers data  
3. Consumer group processes events in parallel  
4. Batch processor writes to PostgreSQL  

---

## Key Concepts Used
- Kafka partitions  
- Consumer groups  
- At-least-once delivery  
- Batch processing  
- Streaming architecture  

---

## How to Run
```bash
docker-compose up -d
python producer.py
python consumer.py

---
## Database Schema

CREATE TABLE user_events (
    user_id INT,
    event TEXT,
    device TEXT,
    timestamp DOUBLE PRECISION
);