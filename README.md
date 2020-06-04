
Steps to install airflow on windows without Docker on the local system

Step 1: Control Panel | Programs and Features | Turn Windows features on or off

Enable : Windows Subsystem for Linux

Step 2: Install Ubuntu from windows store and restart system

Step 3: Install and update PIP

sudo apt-get install software-properties-common
sudo apt-add-repository universe
sudo apt-get update
sudo apt-get install python-pip

Step 4: Install airflow

export SLUGIFY_USES_TEXT_UNIDECODE=yes
pip install apache-airflow

Step 5: Initialize DB 
airflow initdb

Step 6: Start airflow server
airflow webserver -p 8080

Step 7: URL is ready : http://localhost:8080/

Step 8: To run the pipeline
Airflow scheduler

Step 9: On the airflow server UI, Click on “On” for the DAG we want to schedule a new task for.

Step 10: Click on Trigger DAG button 

