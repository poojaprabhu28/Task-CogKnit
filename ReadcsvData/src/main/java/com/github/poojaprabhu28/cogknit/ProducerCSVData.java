package com.github.poojaprabhu28.cogknit;
//Producer pushes data from audio_data.txt to Kafka topic according to genre

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerCSVData {
    public static void main(String[] args) {

        String filepath = "F:\\Programs\\Cogknit\\audio_data.txt";
        String filecontents = "";
        FileReadData fileread = new FileReadData();
        //read file data
        filecontents = fileread.readData(filepath);

        //define topic names and servers
        String bootstrapservers = "127.0.0.1:9092";
        String topic_rap = "Rap";
        String topic_pop = "Pop";
        String topic_rock = "Rock";
        String topic_acoustic = "Acoustic";
        String topic_unknown = "Unknown-genre";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record to push song details into topics
        String[] musicData = filecontents.split("\n");      //file contents split into seperate music record data
        String[] song;      //hols data of 1 song
        String genre = "";      //stores genre data

        //Logger created to log callback data i.e. check whether data has been pushed into topic
        final Logger logger = LoggerFactory.getLogger(ProducerCSVData.class);

        for(String music_data_record : musicData)
        {
            song = music_data_record.split(",");
            genre = song[2];
            //check genre to which the song belongs
            if((genre.equals("Rap")) || (genre.equals("rap"))) {
                //song belongs to Rap genre
                ProducerRecord<String,String> song_record_rap = new ProducerRecord<String, String>(topic_rap,music_data_record);
                producer.send(song_record_rap, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            //error - record not sent
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
            else if((genre.equals("Pop")) || (genre.equals("pop"))) {
                //song belongs to Pop genre
                ProducerRecord<String,String> song_record_pop = new ProducerRecord<String, String>(topic_pop,music_data_record);
                producer.send(song_record_pop, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
            else if((genre.equals("Rock")) || (genre.equals("rock"))) {
                //song belongs to Rock genre
                ProducerRecord<String,String> song_record_rock = new ProducerRecord<String, String>(topic_rock,music_data_record);
                producer.send(song_record_rock, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
            else if((genre.equals("Acoustic")) || (genre.equals("acoustic"))) {
                //song is Acoustic
                ProducerRecord<String,String> song_record_acoustic = new ProducerRecord<String, String>(topic_acoustic,music_data_record);
                producer.send(song_record_acoustic, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
            else{
                //genre of song is not listed
                ProducerRecord<String,String> song_record_unknown = new ProducerRecord<String, String>(topic_unknown,music_data_record);
                producer.send(song_record_unknown, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
        }

        //flush data
        producer.flush();
        //close producer
        producer.close();
    }
}
