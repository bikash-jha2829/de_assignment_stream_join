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
                    
