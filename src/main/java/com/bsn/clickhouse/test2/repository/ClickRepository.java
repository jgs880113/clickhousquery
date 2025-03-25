package com.bsn.clickhouse.test2.repository;

import com.bsn.clickhouse.test2.model.TEST;
import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Repository
@Slf4j
public class ClickRepository {

    @Autowired
    private ConnectionFactory connectionFactory; // 기본 연결 풀

    @Autowired
    private ConnectionFactory anotherConnectionFactory; // 별도의 연결 풀

    public Flux<TEST> getTest(String domain) {
        log.info("getTest query started");
        Instant startTime = Instant.now();
        String sql = "SELECT OCCR_DT, PKT_SEQ, TMSTART, STRTITLE, SIP, DIP, COUNT, SDATA " +
                "FROM TEST " +
                "WHERE OCCR_DT > '2021-01-01'" +
                " AND SIP <> '1'" +
                "ORDER BY DIP DESC LIMIT 100";
        sql = "SELECT OCCR_DT, PKT_SEQ, TMSTART, STRTITLE, SIP, DIP, COUNT, SDATA FROM TEST LIMIT 1";
        String finalSql = sql;
        return Mono.from(anotherConnectionFactory.create())
                .flatMapMany(conn -> {
                    log.info("getTest actual query execution");
                    return Mono.from(conn.createStatement(finalSql).execute()).flux();
                })
                .flatMap(result -> result.map((row, rowMetadata) -> new TEST(
                        row.get("OCCR_DT", String.class),
                        row.get("PKT_SEQ", String.class),
                        row.get("TMSTART", String.class),
                        row.get("STRTITLE", String.class),
                        row.get("SIP", String.class),
                        row.get("DIP", String.class),
                        row.get("COUNT", String.class),
                        row.get("SDATA", String.class)
                )))
                .doOnComplete(() -> {
                    Instant endTime = Instant.now();
                    Duration duration = Duration.between(startTime, endTime);
                    log.info("getTest query completed. Execution time: {} ms", duration.toMillis());
                });
    }

    public Mono<Long> getTestCount() {
        log.info("getTestCount query started");
        Instant startTime = Instant.now();
        String sql = "SELECT COUNT(*) + (SELECT COUNT(*) FROM default.TEST WHERE OCCR_DT > '2021-01-01' AND SIP <> '1')" +
                " + (SELECT COUNT(*) FROM default.TEST WHERE OCCR_DT > '2021-01-01' AND SIP <> '2')" +
                " + (SELECT COUNT(*) FROM default.TEST WHERE OCCR_DT > '2021-01-01' AND SIP <> '3')" +
                " + (SELECT COUNT(*) FROM default.TEST WHERE OCCR_DT > '2021-01-01' AND SIP <> '4')" +
                " FROM TEST" +
                " WHERE OCCR_DT > '2021-01-01'" +
                " AND SIP <> '1'";
        String finalSql = sql;
        return Mono.from(connectionFactory.create())
                .flatMap(conn -> {
                    log.info("getTestCount actual query execution");
                    return Mono.from(conn.createStatement(finalSql).execute());
                })
                .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> row.get(0, Long.class))))
                .single()
                .doOnSuccess(v -> {
                    Instant endTime = Instant.now();
                    Duration duration = Duration.between(startTime, endTime);
                    log.info("getTestCount query completed. Execution time: {} ms", duration.toMillis());
                });
    }
}