package com.pmi.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.UUID;

public class PublishToCCloud {

    public static void main(String...args) throws Exception {

        var config = new HashMap<String, Object>();

        config.put("config.providers", "file,dir,env");
        config.put("config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider");
        config.put("config.providers.dir.class", "org.apache.kafka.common.config.provider.DirectoryConfigProvider");
        config.put("config.providers.env.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");

        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092");
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='${file:/Users/piotr.smolinski/.config/ccloud.conf:username}' password='${file:/Users/piotr.smolinski/.config/ccloud.conf:password}';");

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "ccloud-java-client-4a32d900-c7da-4ea0-a0ff-107d760519f8");

        try (var kafkaProducer = new KafkaProducer<String,String>(config)) {

//            var future = kafkaProducer.send(
//                    new ProducerRecord<String,String>(
//                            "cloudwatch-0",
//                            UUID.randomUUID().toString(),
//                            ResourceUtils.readResource("/windows.event.1.xml")
//                    )
//            );
            var future = kafkaProducer.send(
                    new ProducerRecord<String,String>(
                            "cloudtrail-0",
                            UUID.randomUUID().toString(),
                            ResourceUtils.readResourceAsString("/cloudtrail.record.0.json")
                    )
            );

            kafkaProducer.flush();

            future.get();

        }


    }
}
