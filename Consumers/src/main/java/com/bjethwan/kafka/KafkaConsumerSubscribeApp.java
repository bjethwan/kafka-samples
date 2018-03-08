package com.bjethwan.kafka;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerSubscribeApp  {

public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "ec2-13-250-52-163.ap-southeast-1.compute.amazonaws.com:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("fetch.min.bytes", 1);
		props.put("group.id", "blabla");
		props.put("heartbeat.interval.ms", 3000);
		props.put("max.partition.fetch.bytes", 1048576);
		props.put("session.timeout.ms", 30000);
		props.put("auto.offset.reset", "latest");
		props.put("connections.max.idle.ms", 540000);
		props.put("enable.auto.commit", true);
		props.put("exclude.internal.topics", true);
		props.put("max.poll.records", 2147483647);
		props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
		props.put("request.timeout.ms", 40000);
		props.put("auto.commit.interval.ms", 5000);
		props.put("fetch.max.wait.ms", 500);
		props.put("metadata.max.age.ms", 300000);
		props.put("reconnect.backoff.ms", 50);
		props.put("retry.backoff.ms", 100);
		props.put("client.id", "");


		// Create a KafkaConsumer instance and configure it with properties.
		KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

		// Create a topic subscription list:
		ArrayList<String> topics = new ArrayList<String>();
		topics.add("my-topic"); 
		topics.add("my-other-topic"); 
		
		myConsumer.subscribe(topics);

		// Retrieves the topic subscription list from the SubscriptionState internal object:
		Set<TopicPartition> assignedPartitions = myConsumer.assignment();

		// Print the partition assignments:
		printSet(assignedPartitions);

		// Start polling for messages:
		try {
			while (true){
				ConsumerRecords records = myConsumer.poll(1000);
				printRecords(records);
			}
		} finally {
			myConsumer.close();
		}

	}

	private static void printSet(Set<TopicPartition> collection){
		if (collection.isEmpty()) {
			System.out.println("I do not have any partitions assigned yet...");
		}
		else {
			System.out.println("I am assigned to following partitions:");
			for (TopicPartition partition: collection){
				System.out.println(String.format("Partition: %s in Topic: %s", Integer.toString(partition.partition()), partition.topic()));
			}
		}
	}

	private static void printRecords(ConsumerRecords<String, String> records)
	{
		for (ConsumerRecord<String, String> record : records) {
			System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
		}
	}


}
