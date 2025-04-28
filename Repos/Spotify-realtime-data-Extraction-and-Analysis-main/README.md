# 🎵 Spotify Data Engineering with AWS
## Project Overview

This project explores Spotify's Top 50 - Global playlist using data engineering practices. The primary goal was to extract, transform, and load (ETL) playlist data from Spotify's API and analyze it in the cloud using AWS. With trending songs dominated by Christmas classics, this project offered a fun opportunity to experiment with AWS services while showcasing the timelessness of these hits.

![Top-50](https://github.com/user-attachments/assets/ad02c9e5-e5d9-4d37-ba15-5e638d0273a7)  



## Tech Stack  

The project was built using the following tools and technologies:

![Pipeline Design](https://github.com/user-attachments/assets/dfb43287-9016-440b-a2fe-bf5f9618d12f)  


**Extraction Stage**    
Spotify API: Scraped data from the playlist.    
Amazon CloudWatch: Automated hourly API pulls.    
AWS Lambda: Managed the extraction of raw data.    
Amazon S3: Stored the raw JSON data.

**Transformation Stage**  
AWS Lambda: Transformed raw JSON data into structured data.  
Pandas: Processed data into DataFrames for albums, artists, and songs.  
Amazon S3: Stored the transformed CSV data.

**Load Stage**  
AWS Glue Crawler: Inferred schema for structured data.  
AWS Glue Data Catalog: Managed metadata for querying.  
Amazon Athena: Queried and analyzed transformed data.

## Features 
Automated Data Pipelines: Hourly automation of data extraction using AWS CloudWatch and Lambda.  
Data Transformation: Handled complex JSON structures from Spotify's API, converting them into clean CSVs for analysis.  
Cloud Analysis: Used AWS Glue and Athena for querying the data directly in the cloud.  
Schema Inference: Leveraged AWS Glue Crawler to automatically infer table schemas (with some manual fixes).
