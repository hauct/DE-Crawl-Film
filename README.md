# Crawl movies data and ingest to PostgresDB - Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [What You'll Learn](#what-youll-learn)
- [Technologies](#technologies)
- [Walkthrough](#walkthrough)

## Introduction
The purpose of this project is to design a processing pipeline, starting with the application of the Selenium and Request libraries to automate the crawling of raw data. Then, the data is processed, saved in a tabular form, and ingested into a PostgreSQL database.

## System Architecture
![pipeline_architecture.png](imgs/pipeline_architecture.png)

The project is structured with the following components:
- **Web to crawl**: [here](https://phimmoiiii.net/)
- **Python**: Programming language to design a code to run the pipeline
- **Postgres DB**: Database for storing a table

## System Components
- **pipeline.py**: This is the main task of this project. Start this file to run the pipeline from crawling to ingesting.
- **info.py**: Database information.
- **reqiurements.txt**: Necessary libraries.
- **msedgedriver.exe**: Necessary driver for Selenium

## Walkthrough
1. Open Terminal on your IDE and run this to clone the source code:
    ```bash
    git clone https://github.com/hauct/DE-Crawl-Film.git
    ``` 
2. Install required library and run the python file `pipeline.py`:
    ```bash
    pip install -r reqiurements.txt
    python pipeline.py
    ``` 