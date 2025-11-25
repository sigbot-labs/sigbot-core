package com.wl4g.signaltrading.poc.repository.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * JPA {@link AttributeConverter} for storing {@link Map} as JSON strings.
 */
@Converter
public class PropertiesConverter implements AttributeConverter<Properties, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Properties> MAP_TYPE = new TypeReference<>() {
    };

    @Override
    public String convertToDatabaseColumn(Properties attribute) {
        if (attribute == null || attribute.isEmpty()) {
            return "{}";
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(attribute);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize configuration map to JSON", ex);
        }
    }

    @Override
    public Properties convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isBlank()) {
            return new Properties();
        }
        try {
            return OBJECT_MAPPER.readValue(dbData, MAP_TYPE);
        } catch (IOException ex) {
            throw new IllegalStateException("Could not deserialize configuration JSON to Map", ex);
        }
    }
}

