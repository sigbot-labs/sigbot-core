package com.wl4g.signaltrading.poc.repository;

import com.wl4g.signaltrading.poc.model.StrategyInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StrategyInfoRepository extends JpaRepository<StrategyInfo, Long> {
    java.util.Optional<StrategyInfo> findByNameIgnoreCase(String name);
}

