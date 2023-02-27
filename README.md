# ECC

* Note: Unable to connect to postgres db via my python script 
  * (Though the DB initializes with two tables `eod` and `intraday` generated using the `init.sql` upon launching the docker containers)
  * Launch using docker-compose up -d
  * localhost:8080 endpoint - Adminer GUI to view data in Postgres
  * docker-compose down --volumes
* Using sample csv files under the `input` folder to continue further with business checks(Docker containers are not required)
  * `eod.csv` - Daily end of day report - schematic excerpt of CC050 table
  * `first_intraday_report.csv` - First intraday report extract (ideally would be extracted via a sql query to filter min `time of day` from CI050 table)
* Description and Steps to run Python script - `daily_checks.py`
  * Classes and functions to read from csv, DB and perform required checks
  * `python3 -m smtpd -c DebuggingServer -n localhost:1025` from prject terminal to start debug server to listen and display mail content
  * Run this file to execute the checks
* `margin_types.json` - Holds configurable margin type list to filter out before performing row wise comparison
* `module` holds the code for sample email functionality
* Tests - 