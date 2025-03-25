package com.bsn.clickhouse.config;

import java.time.Duration;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;

@Configuration
public class R2DBCConfig {

    @Value("${clickhouse.host:localhost}")
    private String host;

    @Value("${clickhouse.port:8123}")
    private String port;

    @Value("${clickhouse.database:clickdb}")
    private String database;

    @Value("${clickhouse.user:default}")
    private String user;

    @Value("${clickhouse.password:''}")
    private String password;

    @Bean
    public ConnectionFactory connectionFactory() {
        String url = String.format("r2dbc:clickhouse:http://%s:%s@%s:%s/%s",
                user, password, host, port, database);

        // 기본 ConnectionFactory 생성
        ConnectionFactory connectionFactory = io.r2dbc.spi.ConnectionFactories.get(url);

        // 기본 ConnectionPool 구성
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxSize(10)
                .initialSize(5)
                .maxIdleTime(Duration.ofMinutes(10))
                .build();

        return new ConnectionPool(poolConfig);
    }

    @Bean
    public ConnectionFactory anotherConnectionFactory() {
        String url = String.format("r2dbc:clickhouse:http://%s:%s@%s:%s/%s",
                user, password, host, port, database);

        ConnectionFactory connectionFactory = io.r2dbc.spi.ConnectionFactories.get(url);

        // 별도의 ConnectionPool 구성
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(connectionFactory)
                .maxSize(10)
                .initialSize(5)
                .maxIdleTime(Duration.ofMinutes(10))
                .build();

        return new ConnectionPool(poolConfig);
    }
}
