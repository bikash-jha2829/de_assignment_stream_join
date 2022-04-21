# de_assignment_stream_join
time_taken ~4 hours


![image](https://user-images.githubusercontent.com/79247013/164327840-3136b3ee-6062-4b95-be7e-78d5e87745c9.png)


# Architecture : 
we are recieving json event and consuming it to a different topic (use kafka confluent apis to consume)
based on the records/rows we recived we are running a event handler to do upsert and insert operation in postgres data base
rest api endpoint is exposed (flask api) to fetch the records from db/backend.

## Improvement on architecture : 
1. we could use airflow ( s3_key_sensor-{builtin airflow-operator} or custom filewatcher to monitor if new file arrive and trigger the kafka producer or consumer )
2. instead of kafka confluent since we can perform a join operation at the stream based on  window there - use spark streaming instead.
3. Kubernetes architecture 
    Microservices : i. sparkonk8s-operator to run spark job 
                    ii. Flask restful app
                    iii. service-discovery to pass ini of kafka and postgres

# Code Walkthrough
### src: kafka_queue module 
   **kafka_handlers.py** : it is triggered when we call the kafka subscribe for different events , for example handle_user_events is triggred when we consume user_events from kafka-topic : user_events
   
   **kafka_io.py**  :  kafka logic of creation of topic producer and consumer is written
   
**postgre_etl** : modelling :  contains sql codes for creation of tables
                 
                 _postgre_op.py_ : contains the upsert(_update if exists else insert_)  and insert logic to load data in postgres tables.
                 
                 _postgres_init_ : create a connection parameter for postgres

 **pub_sub_main.py** : used ray library to run kafka producer and kafka consumer in parallel  and create topic if not exists.
 
 **output_api_endpoint** : make http call to _/users/<string:name>_  it call the get_user method of PostgreETL class and query postgresDB to fetch the users.
 
 ### utils :
       **utils.py** : basic load_yaml and run_postgre_query, upsert_logic
       **custom_logger** : used custom logger to enable the logging message and info/error/debug option to a granualar level of a methods inside the class or flie path
                         if we are using kubernetes in future it will enable to pass method and logging level( since this is just an exercise I put the basic function)
                         
### tests: 
        integration test case for kafka
        test case for postgres and upsert logic.
        
     
## Overall improvement points:
1. use of spark streaming to join streams 
2. Postgres database modeeling (good to have a delta tables {raw schema} and perform upsert operation )
3. MIcroservices layer ( need to add service k8s yaml file)
#### priority 1 improvements
4. Add more test cases for rest api and make rest api resilient
5. E2E framework test cases
6. watermarking and checkpointing of kafka streams **
7. addition of poetry






