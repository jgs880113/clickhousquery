package com.bsn.clickhouse.test2.controller;

import com.bsn.clickhouse.test2.model.Click;
import com.bsn.clickhouse.test2.model.ClickStats;
import com.bsn.clickhouse.test2.model.TEST;
import com.bsn.clickhouse.test2.repository.ClickRepository;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@Slf4j
public class Test2Controller {

    @Autowired
    ClickRepository clickRepository;

    @RequestMapping(value = "/test", method = RequestMethod.POST)
    public Mono<List<Map<String, Object>>> getTestWithCount(@RequestBody Map<String, Object> requestParam) {
        Flux<TEST> testData = clickRepository.getTest("dd");
        Mono<Long> testCount = clickRepository.getTestCount();

        return Mono.zip(testData.collectList(), testCount)
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
                                resultMap.put("RN", count); // count 값을 각 맵에 추가
                                return resultMap;
                            })
                            .collect(Collectors.toList());
                });
    }

//    @RequestMapping("/{domain}")
//    public Publisher<List<ClickStats>> getEmployeeById(@PathVariable("domain") String domain) {
//        return Flux.from(clickRepository.getStatsByDomain(domain).collect(Collectors.toList()));
//    }
//
//    @PostMapping
//    @ResponseStatus(HttpStatus.CREATED)
//    public Mono<Void> add(@RequestBody Click click){
//        return Mono.from(clickRepository.add(click));
//    }
}
