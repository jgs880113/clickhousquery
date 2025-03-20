package com.bsn.clickhouse.test2.repository;

import com.bsn.clickhouse.test2.model.Click;
import com.bsn.clickhouse.test2.model.ClickStats;
import com.bsn.clickhouse.test2.model.TEST;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;



@Repository
public class ClickRepository {

    @Autowired
    ConnectionFactory connectionFactory;

    public Flux<TEST> getTest(String domain){
        return Mono.from(connectionFactory.create())
                .flatMapMany(conn -> conn.createStatement("SELECT OCCR_DT, PKT_SEQ, TMSTART, STRTITLE, SIP, DIP, COUNT, SDATA FROM TEST LIMIT 10")
                        //.bind("domain", domain)
                        .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> new TEST(
                        row.get("OCCR_DT", String.class),
                        row.get("PKT_SEQ", String.class),
                        row.get("TMSTART", String.class),
                        row.get("STRTITLE", String.class),
                        row.get("SIP", String.class),
                        row.get("DIP", String.class),
                        row.get("COUNT", String.class),
                        row.get("SDATA", String.class)
                )));
    }

    public Mono<Long> getTestCount() {
        return Mono.from(connectionFactory.create())
                .flatMap(conn -> Mono.from(conn.createStatement("SELECT count(*) FROM TEST").execute()))
                .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> row.get(0, Long.class))))
                .single();
    }

//    public Flux<ClickStats> getStatsByDomain(String domain){
//        return Mono.from(connectionFactory.create())
//                .flatMapMany(conn -> conn.createStatement("select domain, path,  toDate(cdate) as d, count(1) as count from clicks where domain = :domain group by domain, path, d")
//                        .bind("domain", domain)
//                        .execute())
//                .flatMap(result -> result.map((row, rowMetadata) -> new ClickStats(row
//                        .get("domain", String.class), row.get("path", String.class), row.get("d", LocalDate.class),  row.get("count", Long.class))));
//    }
//
//    public Mono<Void> add(Click click){
//        return Mono.from(connectionFactory.create())
//                .flatMapMany(conn -> execute(click, conn)).then();
//    }
//
//    private Publisher<? extends Result> execute(Click click, Connection conn) {
//        return conn.createStatement("insert into clicks values (:domain, :path, :cdate, :count)")
//                .bind("domain", click.getDomain())
//                .bind("path", click.getPath())
//                .bind("cdate", LocalDateTime.now())
//                .bind("count", 1).execute();
//    }

}