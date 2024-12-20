package cn.com.xx.reader;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import tk.mybatis.spring.annotation.MapperScan;

@EnableAsync
@SpringBootApplication
@ComponentScan(basePackages = {"cn.com.xx.reader"})
@EnableScheduling
@MapperScan(value = "cn.com.xx.reader.mapper")
public class ReaderApplication {

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(ReaderApplication.class, args);

    }
}
