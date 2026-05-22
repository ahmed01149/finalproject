package com.weather;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

public class WeatherParquetConsumer {
    private static final String TOPIC = "weather-stations";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int BUFFER_SIZE = 5; 
    private static final List<WeatherMessage> buffer = new ArrayList<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"WeatherMessage\","
            + "\"namespace\":\"com.weather\","
            + "\"fields\":["
            + "  {\"name\":\"stationId\", \"type\":\"long\"},"
            + "  {\"name\":\"sNo\", \"type\":\"long\"},"
            + "  {\"name\":\"batteryStatus\", \"type\":\"string\"},"
            + "  {\"name\":\"statusTimestamp\", \"type\":\"long\"},"
            + "  {\"name\":\"weather\", \"type\":{"
            + "    \"type\":\"record\","
            + "    \"name\":\"Weather\","
            + "    \"fields\":["
            + "      {\"name\":\"humidity\", \"type\":\"int\"},"
            + "      {\"name\":\"temperature\", \"type\":\"int\"},"
            + "      {\"name\":\"windSpeed\", \"type\":\"int\"}"
            + "    ]"
            + "  }}"
            + "]"
            + "}";

    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public static void main(String[] args) {
        
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        System.setProperty("java.library.path", "C:\\hadoop\\bin;" + System.getProperty("java.library.path"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-parquet-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println(" Consumer started... Waiting for messages...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        WeatherMessage weatherMessage = objectMapper.readValue(record.value(), WeatherMessage.class);
                        buffer.add(weatherMessage);

                        if (buffer.size() >= BUFFER_SIZE) {
                            flushToParquet();
                        }
                    } catch (Exception e) {
                        System.err.println(" Error processing record: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void flushToParquet() {
        System.out.println(" Flushing buffer to Parquet...");
        Map<Long, List<WeatherMessage>> partitionedData = new HashMap<>();
        for (WeatherMessage msg : buffer) {
            partitionedData.computeIfAbsent(msg.stationId, k -> new ArrayList<>()).add(msg);
        }

        for (Map.Entry<Long, List<WeatherMessage>> entry : partitionedData.entrySet()) {
            long stationId = entry.getKey();
            List<WeatherMessage> stationRecords = entry.getValue();

        
            long ts = stationRecords.get(0).statusTimestamp;
            java.time.LocalDate date = Instant.ofEpochMilli(ts < 10000000000L ? ts * 1000 : ts)
                    .atZone(ZoneOffset.UTC).toLocalDate();

            String folderPath = String.format("storage/year=%d/month=%02d/day=%02d/station_id=%d",
                date.getYear(), date.getMonthValue(), date.getDayOfMonth(), stationId);
            
            File storageDir = new File(folderPath);
            storageDir.mkdirs();

            String filePath = storageDir.getAbsolutePath() + "/data_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8) + ".parquet";
            
            Configuration conf = new Configuration();
            Path path = new Path(filePath);

            try (ParquetWriter<WeatherMessage> writer = AvroParquetWriter.<WeatherMessage>builder(path)
                .withSchema(AVRO_SCHEMA)
                .withDataModel(ReflectData.get())
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
                    for (WeatherMessage msg : stationRecords) {
                        writer.write(msg);
                    }
                    System.out.println("✅ Success: " + filePath);
                } catch (IOException e) {
                    System.err.println("❌ Write Error: " + e.getMessage());
                }
        }
        buffer.clear();
    }
}