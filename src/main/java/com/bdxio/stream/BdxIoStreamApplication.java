package com.bdxio.stream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Springboot application launcher
 *
 */
@SpringBootApplication
public class BdxIoStreamApplication {

	public static void main(String[] args) {
        SpringApplication application = new SpringApplication(BdxIoStreamApplication.class);
        application.run(args);
    }
	
}
