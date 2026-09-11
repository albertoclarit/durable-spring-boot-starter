package io.github.albertoclarit.durable.internal;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.albertoclarit.durable.DurableJob;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

final class JsonCodec {

    private final ObjectMapper mapper;

    JsonCodec() {
        this.mapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
                .enable(MapperFeature.PROPAGATE_TRANSIENT_MARKER)
                .visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .visibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .build();
    }

    String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize " + (value == null ? "null" : value.getClass().getName()), e);
        }
    }

    <T> T read(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize " + type.getName(), e);
        }
    }

    TypedValue wrap(Object value) {
        if (value == null) {
            return new TypedValue("null", "null");
        }
        return new TypedValue(value.getClass().getName(), write(value));
    }

    Object unwrap(TypedValue value) {
        if (value == null || "null".equals(value.type())) {
            return null;
        }
        try {
            Class<?> type = Class.forName(value.type());
            return mapper.readValue(value.json(), type);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Missing type " + value.type(), e);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize " + value.type(), e);
        }
    }

    <T> T unwrap(TypedValue value, Class<T> type) {
        if (value == null || "null".equals(value.type())) {
            return null;
        }
        try {
            return mapper.readValue(value.json(), type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize " + type.getName(), e);
        }
    }

    String fingerprint(DurableJob<?> job) {
        String payload = job.getClass().getName() + "|" + write(job);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    DurableJob<?> readJob(String jobType, String jobJson) {
        try {
            Class<?> type = Class.forName(jobType);
            if (!DurableJob.class.isAssignableFrom(type)) {
                throw new IllegalStateException(jobType + " is not a DurableJob");
            }
            return (DurableJob<?>) mapper.readValue(jobJson, type);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Missing workflow type " + jobType, e);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize workflow " + jobType, e);
        }
    }

    record TypedValue(String type, String json) {
    }
}
