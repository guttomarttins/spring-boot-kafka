package br.com.cams.service;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import br.com.cams.domain.User;

@Service
public class KafKaProducerService {
	
	private static final Logger logger = LoggerFactory.getLogger(KafKaProducerService.class);

	@Value(value = "${general.topic.name}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Value(value = "${user.topic.name}")
	private String userTopicName;

	@Autowired
	private KafkaTemplate<String, User> userKafkaTemplate;

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void sendMessage(String message) {
		CompletableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topicName, message);
		future.whenComplete(new BiConsumer() {
			@Override
	        public void accept(Object o, Object o2) {
				String s1 = (String) o;
				String s2 = (String) o2;
				System.out.println("s1: " + s1);
				System.out.println("s2: " + s2);
//				logger.info("Sent message: " + message + " with offset: " + o.getRecordMetadata().offset());
	        }
		});
		
//		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
//			@Override
//			public void onSuccess(SendResult<String, String> result) {
//				logger.info("Sent message: " + message + " with offset: " + result.getRecordMetadata().offset());
//			}
//
//			@Override
//			public void onFailure(Throwable ex) {
//				logger.error("Unable to send message : " + message, ex);
//			}
//		});
	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void saveCreateUserLog(User user) {
		String json = user.toString();
		CompletableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(userTopicName, json);
		future.whenComplete(new BiConsumer() {
			@Override
	        public void accept(Object o, Object o2) {
				String s1 = (String) o;
				String s2 = (String) o2;
				System.out.println("s1: " + s1);
				System.out.println("s2: " + s2);
//				logger.info("Sent message: " + message + " with offset: " + o.getRecordMetadata().offset());
	        }
		});
		
//		ListenableFuture<SendResult<String, User>> future = 
//				(ListenableFuture<SendResult<String, User>>) this.userKafkaTemplate.send(userTopicName, user);
//
//		future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
//			@Override
//			public void onSuccess(SendResult<String, User> result) {
//				logger.info("User created: " + user + " with offset: " + result.getRecordMetadata().offset());
//			}
//
//			@Override
//			public void onFailure(Throwable ex) {
//				logger.error("User created : " + user, ex);
//			}
//		});
	}

}
