package br.com.cams.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.com.cams.domain.AppConstants;
import br.com.cams.domain.User;

@Service
public class KafKaProducerService {

	private static final Logger logger = LoggerFactory.getLogger(KafKaProducerService.class);

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	public void sendMessage(String message) {
		logger.info(String.format("Message sent -> %s", message));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME, message);
	}

	public void saveCreateUserLog(User user) {
		logger.info(String.format("User created -> %s", user));
		this.kafkaTemplate.send(AppConstants.TOPIC_NAME_USER_LOG, user);
	}
}
