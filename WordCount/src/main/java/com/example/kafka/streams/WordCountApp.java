package com.example.kafka.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = inputStream.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\s+")))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        //wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.start();

        //print topology
        //System.out.println(kafkaStreams);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
