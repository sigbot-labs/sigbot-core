package com.wl4g.signaltrading.poc.repository;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ExchangeInfoRepository extends JpaRepository<ExchangeInfo, Long> {
    java.util.Optional<ExchangeInfo> findByNameIgnoreCase(String name);
}

