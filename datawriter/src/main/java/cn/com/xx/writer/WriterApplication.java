package cn.com.xx.writer;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
@ComponentScan(basePackages = {"cn.com.daikin.writer"})
public class WriterApplication {

    public static void main(String[] args) {

        org.springframework.boot.SpringApplication.run(WriterApplication.class, args);
    }
}
