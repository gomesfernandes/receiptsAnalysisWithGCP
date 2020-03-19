# Analyse personal receipts with GCP

This repo contains code snippets meant to help me get familiar with GCP Data tools.
So far, it allowed me to play around with the following tools and services:
* Cloud Storage - stores the receipts and intermediate results
* Cloud Functions - triggered when a new receipt is uploaded to CS
* Vision API - used to extract entites from receipts, such as total amount paid or the date
* Cloud Pub/Sub - to publish a message when there's a new file
* Apache Beam, Dataflow - loads the JSON files into BigQuery (initially in batch mode, now in streaming mode)
* BigQuery - stores the final results

