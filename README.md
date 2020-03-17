# Analyse personal receipts with GCP

This repo contains code snippets meant to help me get familiar with GCP Data tools.
So far, it allowed me to play around with the following tools and services:
* Cloud Storage - stores the receipts and intermediate results
* Cloud Functions - triggered when a new receipt is uploaded to CS
* Vision API - used to extract entites from receipts, such as total amount paid or the date
* Apache Beam, Dataflow - transforms the receipts into a JSON file (so far in batch mode, streaming to come)
* BigQuery - loads the final results

