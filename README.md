# Multinational Retail Data Centralisation

## Overview

This project aims to centralise sales data for a multinational retail corporation, streamlining accessibility and analysis for team members. The goal is to establish a single source of truth for sales data by consolidating information from disparate sources into a centralized database.

## Motivation

The primary motivation behind this project is to enhance data accessibility and facilitate data-driven decision-making within the organization. By centralising sales data, team members can easily access accurate and up-to-date information, enabling them to gain deeper insights into sales performance and make informed business decisions.

## Milestones
### Milestone 1: Environment Setup
- Utilised GitHub for version control to ensure code integrity and facilitate collaboration among team members.

### Milestone 2: Data Extraction and Cleaning
- Developed Python scripts for data extraction, cleaning, and database connectivity.
- Extracted data from various sources including CSV files, APIs, S3 buckets, and AWS RDS databases.
- Conducted thorough data cleaning procedures to address NULL values, date errors, and data inconsistencies.

### Milestone 3: Database Schema Creation
- Designed and implemented a star-based database schema using pgAdmin.
- Defined primary keys for dimension tables and established foreign key constraints in the orders table to maintain data integrity.

### Milestone 4: Data Querying
- Utilised SQL queries to extract up-to-date metrics and insights from the centralized database.
- Provided answers to key business questions, such as store distribution, sales patterns, online sales contributions, and staff headcount, facilitating data-driven decision-making.

## Key Achievements
- Successfully centralised sales data from multiple sources into a single database, enabling easy access and analysis for team members.
- Implemented robust data extraction, cleaning, and querying processes, ensuring data accuracy and consistency throughout the project lifecycle.
- Empowered the organization to gain deeper insights into sales performance and make informed business decisions based on data-driven analysis.

## Technologies Used
- Python
- SQL
- pgAdmin
- AWS (S3, RDS)

## Usage
1. Clone the repository to your local machine:
```python
git clone https://github.com/MilanSajiv/Multinational-Retail-Data-Centralisation.git
```

2. Install the required dependencies:
```python
pip install -r requirements.txt
```
3. Execute the desired Python scripts for data extraction, cleaning, and analysis.
```python
python data_extraction.py
python data_cleaning.py
python data_querying.py
```
4. Follow the prompts or configure the scripts as necessary to perform the desired data operations.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
