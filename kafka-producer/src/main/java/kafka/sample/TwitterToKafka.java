/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.sample.producer.AvroTweetProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import avro.sample.twitter.TwitterStatusUpdateConverter;
import ch.trivadis.sample.twitter.avro.v1.TwitterStatusUpdate;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

public class TwitterToKafka  {
	
	private final static Logger logger = LoggerFactory.getLogger(TwitterToKafka.class);

	List<String> followTerms = new ArrayList<String>();
	private int numberOfProcessingThreads = 5; 
	private Client hbcClient;
	private AvroTweetProducer kafkaProducer = new AvroTweetProducer();
	String consumerKey = null;
	String consumerSecret = null;
	String accessToken = null;
	String accessTokenSecret = null;

	public TwitterToKafka(List<String> followTerms, int numberOfProcessingThreads, 
							String consumerKey, String consumerSecret,
							String accessToken, String accessTokenSecret) {
		this.followTerms = followTerms;
		this.numberOfProcessingThreads = numberOfProcessingThreads;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
	}

	public void start() {
		System.out.println("start() ...");
		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		
		// create the endpoint
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(followTerms);
		endpoint.stallWarnings(false);

		// create an authentication
		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		// Create a new BasicClient. By default gzip is enabled.
		ClientBuilder builder = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue));
		hbcClient = builder.build();
		System.out.println("client created ...");

		// Create an executor service which will spawn threads to do the actual
		// work of parsing the incoming messages and
		// calling the listeners on each message
		ExecutorService service = Executors
				.newFixedThreadPool(this.numberOfProcessingThreads);

		// Wrap our BasicClient with the twitter4j client
		Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(hbcClient,
				queue, Lists.newArrayList(listener), service);

		// Establish a connection
		t4jClient.connect();
		System.out.println("connection established ...");

		for (int threads = 0; threads < this.numberOfProcessingThreads; threads++) {
			// This must be called once per processing thread
			t4jClient.process();
			System.out.println("thread " + threads + " started ...");

		}
	};

	public void stop() {
		hbcClient.stop();
	};

	public boolean isRunning() {
		return true;
	};
	
	// A bare bones StatusStreamHandler, which extends listener and gives some
	// extra functionality
	private StatusListener listener = new StatusStreamHandler() {

		@Override
		public void onStatus(Status status) {
			if (status == null) {
				System.err.println("status is null");
			} else {

				try {
			        TwitterStatusUpdate statusAvro = TwitterStatusUpdateConverter.convert(status);

			        kafkaProducer.produce(statusAvro);
				} catch (Exception e) {
					logger.error("Error occured in onStatus()", e);
					e.printStackTrace();
				}
			}
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			//too many, do not log
			logger.info("=====> onDeletionNotice: " + statusDeletionNotice);
		}

		@Override
		public void onTrackLimitationNotice(int limit) {
			logger.warn("=====> onTrackLimitationNotice: " + limit);
		}

		@Override
		public void onScrubGeo(long user, long upToStatus) {
			logger.warn("=====> onScrubGeo: " + user);
		}

		@Override
		public void onException(Exception e) {
			logger.error("=====> onException", e);
		}

		@Override
		public void onDisconnectMessage(DisconnectMessage message) {
			logger.error("=====> onDisconnectMessage: " + message);
		}

		@Override
		public void onUnknownMessageType(String s) {
			logger.warn("=====> onUnknownMessageType: " + s);
		}

		@Override
		public void onStallWarning(StallWarning arg0) {
			logger.warn("=====> onStallWarning: " + arg0);
		}

		@Override
		public void onStallWarningMessage(StallWarningMessage arg0) {
			logger.warn("=====> onStallWarningMessage: " + arg0);
		}

	};

	public static void main(String[] args) {
		// Twitter Terms to follow
		List<String> followTerms = new ArrayList<String>();
		followTerms.add("#bigdata");
		followTerms.add("#nosql");
		followTerms.add("#cloud");
		followTerms.add("#iot");

		// Twitter Authentication
		String consumerKey = "TRbhBcCp8QesXqcIDQMMGwk01";
		String consumerSecret = "VlBjyWyQK5XgQJpHXUIvTkRHlRZYSwuIbTs1UO3GGmEJleG2Aw";
		String accessToken = "18898576-kKEEFsiwiDDt3n4uSqSMu8yd2Tbaarxyemc83alC2";
		String accessTokenSecret = "eBYm8gRCZgUvpW4jcuYZcji9BRMYMVleKrUSLrpKwpakX";
		
		// number of threads to use for retrieving the tweets from twitter
		int numberOfThreads = 3;

		TwitterToKafka twitterToKafka = new TwitterToKafka(followTerms, numberOfThreads, consumerKey, consumerSecret, accessToken, accessTokenSecret);
		twitterToKafka.start();
	}

}
