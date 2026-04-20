package net.ayoub;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class Exercice2 {
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

        KStream<String,String> FiltredDataByTemperature = meterologieStrem.filter((key, value) ->{
            String[] parts  = value.split(",");
            Double Temperature = Double.parseDouble(parts[1]);
            return Temperature>30.0;
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
        KTable<String,Double> Temperature_average_by_station = groupedStream.aggregate(() ->0.0 ,
                (key,value,aggregate)->{
                        double temperature = Double.parseDouble(value.split(",")[1]);



                });



    }
}
