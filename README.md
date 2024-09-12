# Replicate To Iceberg

rep2ib replicates a database to Apache Iceberg. 

## Description

rep2ib works in 3 stages:
1. Read configuration files. By default it gets configuration file from config/default.json.
1. Geneate Apache Arrow table schema for the table to be replicated.
1. Read a batch of records from the table.
1. Write the batch to Iceberg.

## Authors

Liang Chen