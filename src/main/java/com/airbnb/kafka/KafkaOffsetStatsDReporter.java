/*
 * Copyright (c) 2015.  Airbnb.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.airbnb.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.airbnb.kafka.KafkaAdminClient.KafkaOffset;
import com.timgroup.statsd.StatsDClient;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import scala.Tuple2;

/**
 *
 */
public class KafkaOffsetStatsDReporter extends AbstractPollingReporter {
	static final Logger log = LoggerFactory.getLogger(KafkaOffsetStatsDReporter.class);
	public static final String REPORTER_NAME = "kafka-statsd-offset";

	private final StatsDClient statsd;
	private final KafkaAdminClient kafkaAdminClient;
	private final Clock clock;
	private boolean isTagEnabled;

	public KafkaOffsetStatsDReporter(MetricsRegistry metricsRegistry, KafkaAdminClient kafkaAdminClient,
			StatsDClient statsd, boolean isTagEnabled) {
		super(metricsRegistry, REPORTER_NAME);
		this.statsd = statsd; // exception in statsd is handled by default NO_OP_HANDLER (do nothing)
		this.clock = Clock.defaultClock();
		this.isTagEnabled = isTagEnabled;
		this.kafkaAdminClient = kafkaAdminClient;
	}

	@Override
	public void run() {
		try {
			sendAllKafkaOffsets();
		} catch (RuntimeException ex) {
			log.error("Failed to print offsets to statsd", ex);
		}
	}

	private void sendAllKafkaOffsets() {
		final List<String> groups = kafkaAdminClient.getAllGroups();
		for (String group : groups) {
			Map<String, Tuple2<Long, Long>> countOffsetMap = new HashMap<>();
			List<KafkaOffset> offsets = kafkaAdminClient.getOffsets(group);
			for(KafkaOffset offset : offsets) {
				String name = group + "." + offset.getTopicPartition().topic() + "." + offset.getTopicPartition().partition();
				sendMetric(name + ".offset", offset.getOffset());
				sendMetric(name + ".endOffset", offset.getEndOffset());
				String key = group + "." + offset.getTopicPartition().topic();
				Tuple2<Long, Long>  tuple = countOffsetMap.get(key);
				if(tuple == null) {
					tuple = new Tuple2<Long, Long>(offset.getOffset(), offset.getEndOffset());
					countOffsetMap.put(key, tuple);
				} else {
					tuple = new Tuple2<Long, Long>(tuple._1 + offset.getOffset(), tuple._2 + offset.getEndOffset());
					countOffsetMap.put(key, tuple);
				}
				for(Entry<String, Tuple2<Long, Long>> entry : countOffsetMap.entrySet()) {
					sendMetric(entry.getKey() +".all" + ".offset", entry.getValue()._1);
					sendMetric(entry.getKey() +".all" + ".endOffset", entry.getValue()._1);
				}
			}
		}
	}

	private void sendMetric(String metricName, Long metric) {
		log.debug("metricName[{}], metric[{}].", metricName, metric);

		try {
			statsd.gauge(metricName, metric);
		} catch (Exception ignored) {
			log.error("Error printing regular metrics:", ignored);
		}
	}

}
