package com.wl4g.signaltrading.poc.controller;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.service.exchange.IExchangeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/exchanges")
public class ExchangeController {

    private final IExchangeService exchangeService;

    @PostMapping
    public ResponseEntity<ExchangeInfo> createExchange(@Valid @RequestBody ExchangeInfo exchangeInfo) {
        return ResponseEntity.status(HttpStatus.CREATED).body(exchangeService.create(exchangeInfo));
    }

    @PutMapping("/{exchangeId}")
    public ResponseEntity<ExchangeInfo> updateExchange(@PathVariable Long exchangeId,
                                                       @Valid @RequestBody ExchangeInfo exchangeInfo) {
        return ResponseEntity.ok(exchangeService.update(exchangeId, exchangeInfo));
    }

    @DeleteMapping("/{exchangeId}")
    public ResponseEntity<Void> deleteExchange(@PathVariable Long exchangeId) {
        exchangeService.delete(exchangeId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{exchangeId}")
    public ResponseEntity<ExchangeInfo> getExchange(@PathVariable Long exchangeId) {
        return ResponseEntity.ok(exchangeService.get(exchangeId));
    }

    @GetMapping
    public ResponseEntity<List<ExchangeInfo>> listExchanges() {
        return ResponseEntity.ok(exchangeService.getAll());
    }
}

