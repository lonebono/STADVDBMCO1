# MCO1 STADVDB

## This is our MCO1 Project with OLAP connected to Supabase PostgreSQL database

It is assumed you are following the same folder structure in this repository

1. Clone the repository using the command
   git clone https://github.com/lonebono/STADVDBMCO1.git
2. Install Python environment
   pip install -r requirements.txt
3. Set up your local Postgres (include all packages iirc)
4. Connect to your local database instance
   ex. psql -h localhost -U postgres -d mco1_imdb
5. Create tables in your pgAdmin using raw_tables.sql
6. Use the ETL script to stage and populate the data warehouse
   python etl\etl.py
7.
