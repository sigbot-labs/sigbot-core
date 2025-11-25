package com.wl4g.signaltrading.poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//@EntityScan(basePackages = {"com.wl4g.signaltrading.poc.model"})
@SpringBootApplication
public class SignalTradingPoC {
    public static void main(String[] args) {
        SpringApplication.run(SignalTradingPoC.class, args);
    }
}
