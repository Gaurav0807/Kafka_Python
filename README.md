# Kafka_Python


# Steps:-
. Create Virtual Environment in Folder (venv\Scripts\activate) <br />
. Install pip install kafka-python <br />
. Install all the imports that required <br />

. Run: docker-compose up --build <br />
. Then run producer.py and consumer.py <br />


# Kafka Producer :- Produces the data
<p align="center">
  <img src="https://github.com/Gaurav0807/Kafka_Python/blob/master/Learning_Kafka/Images/Kafka_Producer.png" width="350" title="hover text"> 
</p>


# Kafka Consumer :- Consumer the data

<p align="center">
  <img src="https://github.com/Gaurav0807/Kafka_Python/blob/master/Learning_Kafka/Images/Kafka_Consumer.png" width="350" title="hover text"> 
</p>


# Multiple Consumer 
. When we run multiple consumer then any consumer can start to consume but if any other consumer1 stop then consumer2 will start consuming it .
(Note :- group_id should be same for both consumer) 
