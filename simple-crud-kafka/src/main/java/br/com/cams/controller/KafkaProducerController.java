package br.com.cams.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import br.com.cams.domain.User;
import br.com.cams.service.KafKaProducerService;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaProducerController {

	@Autowired
	private KafKaProducerService producerService;
	
	@PostMapping(value = "/publish")
	public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
		this.producerService.sendMessage(message);
	}

	@PostMapping(value = "/createUser")
	public void sendMessageToKafkaTopic(
			@RequestParam("userId") long userId,
			@RequestParam("firstName") String firstName,
			@RequestParam("lastName") String lastName) {

		User user = new User(userId, firstName,lastName);
		this.producerService.saveCreateUserLog(user);
	}
 
}