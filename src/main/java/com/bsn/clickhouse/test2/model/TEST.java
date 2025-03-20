package com.bsn.clickhouse.test2.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;

@Slf4j
@Data
@NoArgsConstructor // 기본 생성자 자동 생성
@AllArgsConstructor // 모든 속성을 매개변수로 받는 생성자 자동 생성
public class TEST {
    String OCCR_DT;
    String  PKT_SEQ;
    String TMSTART;
    String  STRTITLE;
    String SIP;
    String  DIP;
    String COUNT;
    String SDATA;
}
