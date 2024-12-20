package cn.com.xx.writer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfig {

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(String group) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3); // 核心线程数
        executor.setMaxPoolSize(5); // 最大线程数
        executor.setQueueCapacity(100); // 队列大小
        executor.setThreadNamePrefix("thread-"+group); // 线程前缀
        executor.initialize();
        return executor;
    }
}

