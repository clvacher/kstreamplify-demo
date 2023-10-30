package com.demo.kstreamplify;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Springboot application launcher
 *
 */
@SpringBootApplication
public class ErrorHandlingStreamApplication {

	public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ErrorHandlingStreamApplication.class);
        application.run(args);
    }
}
