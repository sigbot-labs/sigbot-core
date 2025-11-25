package com.wl4g.signaltrading.poc.service.strategy;

import com.wl4g.signaltrading.poc.model.StrategyInfo;
import com.wl4g.signaltrading.poc.repository.StrategyInfoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * JPA backed {@link IStrategyService} implementation.
 */
@Service
@Primary
@RequiredArgsConstructor
@Transactional
public class JpaStrategyService implements IStrategyService {

    private final StrategyInfoRepository repository;

    @Override
    public StrategyInfo create(StrategyInfo strategyInfo) {
        StrategyInfo payload = prepareForPersist(requireNonNull(strategyInfo, "strategyInfo"));
        repository.findByNameIgnoreCase(payload.getName())
                .ifPresent(existing -> {
                    throw new IllegalArgumentException("Strategy name already exists: " + existing.getName());
                });
        payload.setId(null);
        return repository.save(payload);
    }

    @Override
    public StrategyInfo update(Long strategyId, StrategyInfo strategyInfo) {
        StrategyInfo existing = get(strategyId);
        StrategyInfo payload = prepareForPersist(requireNonNull(strategyInfo, "strategyInfo"));
        if (payload.getId() != null && !Objects.equals(payload.getId(), strategyId)) {
            throw new IllegalArgumentException("Strategy ID mismatch between path and payload");
        }
        repository.findByNameIgnoreCase(payload.getName())
                .filter(found -> !Objects.equals(found.getId(), existing.getId()))
                .ifPresent(found -> {
                    throw new IllegalArgumentException("Strategy name already exists: " + found.getName());
                });
        existing.setName(payload.getName());
        existing.setProvider(payload.getProvider());
        existing.setDescription(payload.getDescription());
        existing.setParameters(payload.getParameters());
        return repository.save(existing);
    }

    @Override
    public void delete(Long strategyId) {
        StrategyInfo existing = get(strategyId);
        repository.delete(existing);
    }

    @Override
    @Transactional(readOnly = true)
    public StrategyInfo get(Long strategyId) {
        requireNonNull(strategyId, "strategyId");
        return repository.findById(strategyId)
                .orElseThrow(() -> new IllegalArgumentException("Strategy not found: " + strategyId));
    }

    @Override
    @Transactional(readOnly = true)
    public List<StrategyInfo> getAll() {
        return repository.findAll();
    }

    private StrategyInfo prepareForPersist(StrategyInfo strategyInfo) {
        strategyInfo.validate();
        if (strategyInfo.getParameters() == null) {
            throw new IllegalArgumentException("Strategy parameters must not be null");
        }
        return strategyInfo;
    }
}

