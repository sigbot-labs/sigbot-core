package com.wl4g.signaltrading.poc.controller;

import com.wl4g.signaltrading.poc.model.StrategyInfo;
import com.wl4g.signaltrading.poc.service.strategy.IStrategyService;
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
@RequestMapping("/api/strategies")
public class StrategyController {

    private final IStrategyService strategyService;

    @PostMapping
    public ResponseEntity<StrategyInfo> createStrategy(@Valid @RequestBody StrategyInfo strategyInfo) {
        return ResponseEntity.status(HttpStatus.CREATED).body(strategyService.create(strategyInfo));
    }

    @PutMapping("/{strategyId}")
    public ResponseEntity<StrategyInfo> updateStrategy(@PathVariable Long strategyId,
                                                       @Valid @RequestBody StrategyInfo strategyInfo) {
        return ResponseEntity.ok(strategyService.update(strategyId, strategyInfo));
    }

    @DeleteMapping("/{strategyId}")
    public ResponseEntity<Void> deleteStrategy(@PathVariable Long strategyId) {
        strategyService.delete(strategyId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{strategyId}")
    public ResponseEntity<StrategyInfo> getStrategy(@PathVariable Long strategyId) {
        return ResponseEntity.ok(strategyService.get(strategyId));
    }

    @GetMapping
    public ResponseEntity<List<StrategyInfo>> listStrategies() {
        return ResponseEntity.ok(strategyService.getAll());
    }
}

