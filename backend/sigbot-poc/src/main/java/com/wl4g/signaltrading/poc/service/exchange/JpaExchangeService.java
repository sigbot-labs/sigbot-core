package com.wl4g.signaltrading.poc.service.exchange;

import com.wl4g.signaltrading.poc.model.ExchangeInfo;
import com.wl4g.signaltrading.poc.repository.ExchangeInfoRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * JPA backed {@link IExchangeService} implementation.
 */
@Service
@Primary
@RequiredArgsConstructor
@Transactional
public class JpaExchangeService implements IExchangeService {

    private final ExchangeInfoRepository repository;

    @Override
    public ExchangeInfo create(ExchangeInfo exchangeInfo) {
        ExchangeInfo toPersist = prepareForPersist(requireNonNull(exchangeInfo, "exchangeInfo"));
        repository.findByNameIgnoreCase(toPersist.getName())
                .ifPresent(existing -> {
                    throw new IllegalArgumentException("Exchange name already exists: " + existing.getName());
                });
        toPersist.setId(null);
        return repository.save(toPersist);
    }

    @Override
    public ExchangeInfo update(Long exchangeId, ExchangeInfo exchangeInfo) {
        ExchangeInfo existing = get(exchangeId);
        ExchangeInfo payload = prepareForPersist(requireNonNull(exchangeInfo, "exchangeInfo"));
        if (payload.getId() != null && !Objects.equals(payload.getId(), exchangeId)) {
            throw new IllegalArgumentException("Exchange ID mismatch between path and payload");
        }
        repository.findByNameIgnoreCase(payload.getName())
                .filter(found -> !found.getId().equals(existing.getId()))
                .ifPresent(found -> {
                    throw new IllegalArgumentException("Exchange name already exists: " + found.getName());
                });
        existing.setName(payload.getName());
        existing.setProvider(payload.getProvider());
        existing.setDescription(payload.getDescription());
        existing.setConfiguration(payload.getConfiguration());
        return repository.save(existing);
    }

    @Override
    public void delete(Long exchangeId) {
        ExchangeInfo existing = get(exchangeId);
        repository.delete(existing);
    }

    @Override
    @Transactional(readOnly = true)
    public ExchangeInfo get(Long exchangeId) {
        requireNonNull(exchangeId, "exchangeId");
        return repository.findById(exchangeId)
                .orElseThrow(() -> new IllegalArgumentException("Exchange not found: " + exchangeId));
    }

    @Override
    @Transactional(readOnly = true)
    public List<ExchangeInfo> getAll() {
        return repository.findAll();
    }

    private ExchangeInfo prepareForPersist(ExchangeInfo exchangeInfo) {
        exchangeInfo.validate();
        if (exchangeInfo.getConfiguration() == null) {
            throw new IllegalArgumentException("Exchange configuration must not be null");
        }
        return exchangeInfo;
    }
}

