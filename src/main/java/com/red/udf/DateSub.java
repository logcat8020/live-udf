package com.red.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static java.time.format.DateTimeFormatter.ofPattern;


public class DateSub extends ScalarFunction {
    private static final DateTimeFormatter formatter = ofPattern("yyyyMMdd");

    public String eval(String dtm, Integer day) {
        try {
            LocalDate date = LocalDate.parse(dtm, formatter);
            LocalDate result = date.minusDays(day);
            return result.format(formatter);
        } catch (Exception e) {
            return null;
        }
    }
}
