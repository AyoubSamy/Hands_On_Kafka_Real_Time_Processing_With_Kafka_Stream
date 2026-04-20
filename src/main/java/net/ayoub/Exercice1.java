package net.ayoub;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Exercice1 {
    public static void main(String[] args) {
        // Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        Set<String> forbiddenWords = Set.of("HACK", "SPAM", "XXX");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> sourceStream = builder.stream("text-input");

        KStream<String,String> Text_cleaned = sourceStream
                .mapValues(value -> value
                        .trim()
                        .replaceAll("\\s+", " ")
                        .toUpperCase()
                );

// separer les messages qui sont valide et invalide en utilisant split

        Map<String, KStream<String, String>> branches = Text_cleaned.split(Named.as("filter-"))
                .branch((key, value) -> !value.isBlank()
                                && forbiddenWords.stream().noneMatch(value::contains)
                                && value.length() <= 100,
                        Branched.as("valid"))
                .branch((key, value) -> true, Branched.as("invalid"))
                .noDefaultBranch();

        branches.get("filter-valid").to("text-clean");
        branches.get("filter-invalid").to("text-dead-letter");


        // Démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajouter un hook pour arrêter proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}