import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

	public static void main(String[] args) {
		
		Properties configProps = new Properties();
		configProps.setProperty("bootstrap.servers", "ec2-54-254-203-132.ap-southeast-1.compute.amazonaws.com:9092");
		configProps.setProperty("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer");
		configProps.setProperty("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(configProps);
		
		ProducerRecord<String, String> msg = 
				new ProducerRecord<>(
						"mib1", 																//Topic 
						"This is from sample kafka-clients based Java program");				//Value
		
		producer.send(msg);
		
		producer.close();
		
		System.out.println("Message Sent!!!");
	}

}
