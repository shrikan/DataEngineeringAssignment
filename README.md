**Data Engineering Assignment:**

Assignment is about gathering data from various sources in the company, enrich the collected data and deliver it to stakeholders.  


**Requirements:**
* Programing language: Java
* Processing framework: Spark
* Queuing technology: Kafka
* Database technology: Postgres.


**Database tables:**  
`driver (id BIGINT, date_created TIMESTAMP, name CHARACTER VARYING(255))`  
`passenger (id BIGINT, date_created TIMESTAMP, name CHARACTER VARYING(255))`  
`booking ( id BIGINT, date_created TIMESTAMP, id_driver BIGINT, id_passenger BIGINT, rating INT, start_date TIMESTAMP, end_date TIMESTAMP,  tour_value BIGINT)`  

**Tasks:**
* Task 01:
    Given the DDL above, create a full database model including keys, constraints or any other requirement necessary.  
    Upload the csv files to local postgres database.
* Task 02:
    Based on new database, create a report listing the TOP 10 drivers from 2016.  
    Drivers with a high tour value and a good average rating are ranked high.
* Task 03:
    Relationships between drivers and passengers.  
    Provide a list of the TOP 10 strongest relationships between passengers and drivers. This relationship is defined by the     number of tours they did together.
* Task 04: 
    Provide a KPI report containing the yearly figures for bookings, the average driver evaulation and revenue for 2016.  
    Print those results to the log as well.  
* Task 05:
    The last task is to deliver these KPIs to our Management Reporting Tool.  
    This tool can fetch data from our Kafka Queue. Thus setup Kafka Broker locally and publish the results of Task 02, Task     03 and Task 04 to different kafka topic.
   
    
