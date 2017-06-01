package com.airbnb.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

import kafka.admin.AdminClient;
import kafka.admin.AdminClient.ConsumerSummary;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.coordinator.GroupOverview;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

public class KafkaAdminClient {

	private String defaultBootstrapServers;
	
	private ZkUtils zkUtils;
	private AdminClient adminClient;
	
	public KafkaAdminClient(String zkConnect, String defaultBootstrapServers) {
		this.defaultBootstrapServers = defaultBootstrapServers;
		zkUtils = ZkUtils.apply(zkConnect, 30000, 30000, false);
		
		Properties config = new Properties();
		config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
		adminClient = AdminClient.create(config);
	}
	
	public int getController() {
		return zkUtils.getController();
	}
	
	public List<String> getAllTopics() {
		return JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
	}
	
	public void createTopic(String topic, int partitions, int replicas) {
		RackAwareMode rackAwareMode =  new RackAwareMode.Enforced$();
		AdminUtils.createTopic(zkUtils, topic, partitions, replicas, new Properties(), rackAwareMode);
	}
	
	public TopicMetadata getTopicMetadata(String topic) {
		return AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
	}
	
	public List<Node> getAllBrokerNodes() {
		List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
		List<Node> nodes = new ArrayList<>();
		for(Broker broker : brokers) {
			Node node = broker.getNode(SecurityProtocol.PLAINTEXT);
			nodes.add(node);
		}
		return nodes;
	}
	
	public Set<TopicAndPartition> getAllPartitions() {
		return JavaConversions.setAsJavaSet(zkUtils.getAllPartitions());
	}
	
	public String getBootstrapServers() {
		StringBuilder sb = new StringBuilder();
		for(Node node : getAllBrokerNodes()) {
			sb.append(node.host()).append(":").append(node.port()).append(",");
		}
		if(sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.length() == 0 ? defaultBootstrapServers : sb.toString();
	}
	
	public List<String> getAllGroups() {
		List<GroupOverview> overviews = JavaConversions.seqAsJavaList(adminClient.listAllConsumerGroupsFlattened());
		List<String> groups = new ArrayList<>();
		for(GroupOverview overview : overviews) {
			groups.add(overview.groupId());
		}
		return groups;
	}

	public List<TopicPartition> getGroupPartitions(String group) {
		List<ConsumerSummary> summarys = 
				JavaConversions.seqAsJavaList(adminClient.describeConsumerGroup(group).get());
		List<TopicPartition> topicPartitions = new ArrayList<>();
		for(ConsumerSummary summary : summarys) {
			List<TopicPartition> parts = JavaConversions.seqAsJavaList(summary.assignment());
			topicPartitions.addAll(parts);
		}
		return topicPartitions;
	}
	
	public List<KafkaOffset> getOffsets(String group) {
		List<KafkaOffset> offsets = new ArrayList<>();
		KafkaConsumer<String, String> consumer = createConsumer(group);
		try {
			List<TopicPartition> topicPartitions = getGroupPartitions(group);
			for(TopicPartition topicPartition : topicPartitions) {
				long offset = getOffset(consumer, topicPartition);
				long endOffset = getEndOffset(consumer, topicPartition);
				offsets.add(new KafkaOffset(topicPartition, offset, endOffset));
			}
		} finally {
			if(consumer != null) {
				consumer.close();
			}
		}
		return offsets;
	}
	
	public void close() {
		if(adminClient != null) {
			adminClient.close();
		}
		if(zkUtils != null) {
			zkUtils.close();
		}
	}

	private KafkaConsumer<String, String> createConsumer(String group) {
		Properties config = new Properties();
		config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
		config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
		return consumer;
	}

	private long getOffset(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
		OffsetAndMetadata offset = consumer.committed(topicPartition);
		long currentOffset = offset.offset();
		return currentOffset;
	}
	
	private long getEndOffset(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
		consumer.assign(Arrays.asList(topicPartition));
		consumer.seekToEnd(Arrays.asList(topicPartition));
		long logEndOffset = consumer.position(topicPartition);
		return logEndOffset;
	}
	
	public static class KafkaOffset {
		private TopicPartition topicPartition;
		private long offset;
		private long endOffset;
		
		public KafkaOffset(TopicPartition topicPartition, long offset, long endOffset) {
			this.topicPartition = topicPartition;
			this.offset = offset;
			this.endOffset = endOffset;
		}
		
		public TopicPartition getTopicPartition() {
			return topicPartition;
		}
		
		public long getOffset() {
			return offset;
		}
		
		public long getEndOffset() {
			return endOffset;
		}
	}
	
	
	public static void main(String[] args) {
		KafkaAdminClient client = new KafkaAdminClient("localhost:2181", "localhost:9092");
		System.out.println(client.getBootstrapServers());
		System.out.println(client.getController());
		System.out.println(client.getAllTopics());
		System.out.println(client.getAllGroups());
		System.out.println(client.getGroupPartitions("console-consumer-34828"));
		System.out.println(client.getTopicMetadata("test2").partitionMetadata().get(1).replicas());
		client.createTopic("test2", 5, 1);
		System.out.println(client.getAllTopics());
	}
}
