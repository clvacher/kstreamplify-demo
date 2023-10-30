package com.demo.kstreamplify;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Springboot application launcher
 *
 */
@SpringBootApplication
public class BaseStreamApplication {

	public static void main(String[] args) {
        SpringApplication application = new SpringApplication(BaseStreamApplication.class);
        application.run(args);
    }
}
