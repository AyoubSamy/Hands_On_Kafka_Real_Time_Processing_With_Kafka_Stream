package net.ayoub;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class Meteologic_Data {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        StreamsBuilder builder = new StreamsBuilder();
// 1. Lire les données météorologiques  depuis le topic Kafka 'weather-data'
//en utilisant un flux (KStream).
        KStream<String,String> meterologieStrem = builder.stream("weather-data");

 // 2 Transformation 1 : eliminer les donnees avec les temperature moins que 30 degre
        KStream<String,String> FiltredDataByTemperature = meterologieStrem.filter((key, value) -> {
            // 1. Check if the value is null or empty
            if (value == null || value.trim().isEmpty()) return false;

            // 2. Split and check if we have at least the first two columns
            String[] parts = value.split(",");
            if (parts.length < 2) {
                System.out.println("Skipping malformed line: " + value);
                return false;
            }

            try {
                Double Temperature = Double.parseDouble(parts[1]);
                return Temperature > 30.0;
            } catch (NumberFormatException e) {
                System.err.println("Non-numeric temperature found: " + parts[1]);
                return false;
            }
        });
//3 Transformation 2 : convertire la temperature en Fahrenheit (°F)

        KStream<String,String> MeterologiqueDataConverted = FiltredDataByTemperature.mapValues(value -> {
            String[] parts = value.split(",");
            String Station = parts[0];
            String Humidite = parts[2];
            Double Temperature = Double.parseDouble(parts[1]);
            Double TemperatureConverted = (Temperature*9/5)+32;
            return Station + "," + TemperatureConverted + "," + Humidite;
        });


        KGroupedStream<String,String> groupedStream = MeterologiqueDataConverted.groupBy((key, value) ->value.split(",")[0] );
        KTable<String, String> averageStats = groupedStream.aggregate(
                () -> "0.0:0.0:0", // Initial "sumTemp:sumHumidity:count"
                (key, value, aggregate) -> {
                    String[] data = value.split(",");
                    double temp = Double.parseDouble(data[1]);
                    double hum = Double.parseDouble(data[2]);

                    String[] parts = aggregate.split(":");
                    double newSumTemp = Double.parseDouble(parts[0]) + temp;
                    double newSumHum = Double.parseDouble(parts[1]) + hum;
                    int newCount = Integer.parseInt(parts[2]) + 1;

                    return newSumTemp + ":" + newSumHum + ":" + newCount;
                }
        ).mapValues((key,value) -> {
            String[] parts = value.split(":");
            double avgTemp = Double.parseDouble(parts[0]) / Double.parseDouble(parts[2]);
            double avgHum = Double.parseDouble(parts[1]) / Double.parseDouble(parts[2]);
            return key + " / " + String.format("%.2f", avgTemp) + " / " + String.format("%.2f", avgHum);
        });

        averageStats.toStream().to("weather-averages");
        // Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
