package com.bsn.clickhouse.test2.controller;

import com.bsn.clickhouse.test2.model.TEST;
import com.bsn.clickhouse.test2.repository.ClickRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@Slf4j
public class Test2Controller {

    @Autowired
    ClickRepository clickRepository;

    @RequestMapping(value = "/test2", method = RequestMethod.POST)
    public Mono<List<Map<String, Object>>> getTestWithCount(@RequestBody Map<String, Object> requestParam) {
        Instant startTime = Instant.now();

        // getTest 쿼리와 getTestCount 쿼리를 병렬로 실행합니다.
        Mono<List<TEST>> testDataMono = clickRepository.getTest("dd").collectList().publishOn(Schedulers.boundedElastic());
        Mono<Long> testCountMono = clickRepository.getTestCount().publishOn(Schedulers.boundedElastic());

        // 두 쿼리의 결과를 결합합니다.
        return Mono.zip(testDataMono, testCountMono)
                .map(tuple -> {
                    List<TEST> tests = tuple.getT1();
                    Long count = tuple.getT2();

                    return tests.stream()
                            .map(test -> {
                                Map<String, Object> resultMap = new HashMap<>();
                                resultMap.put("OCCR_DT", test.getOCCR_DT());
                                resultMap.put("PKT_SEQ", test.getPKT_SEQ());
                                resultMap.put("TMSTART", test.getTMSTART());
                                resultMap.put("STRTITLE", test.getSTRTITLE());
                                resultMap.put("SIP", test.getSIP());
                                resultMap.put("DIP", test.getDIP());
                                resultMap.put("COUNT", test.getCOUNT());
                                resultMap.put("SDATA", test.getSDATA());
                                resultMap.put("RN", count);
                                return resultMap;
                            })
                            .collect(Collectors.toList());
                })
                .doOnSuccess(result -> {
                    Instant endTime = Instant.now();
                    Duration duration = Duration.between(startTime, endTime);
                    log.info("Total execution time: {} ms", duration.toMillis());
                });
    }
}