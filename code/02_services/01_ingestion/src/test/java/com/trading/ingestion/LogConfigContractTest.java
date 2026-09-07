package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * P1-132/P1-134 fail-fast guards: the JSON log must carry writer identity in
 * its filename (shared volume) and the contract's {@code environment} key —
 * plain-XML asserts so a future config edit fails the build, not prod.
 */
final class LogConfigContractTest {

    private static String log4j2Xml() throws Exception {
        try (var in = LogConfigContractTest.class.getResourceAsStream("/log4j2.xml")) {
            assertTrue(in != null, "log4j2.xml must be on the classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void jsonLogFilenameCarriesWriterIdentity() throws Exception {
        String xml = log4j2Xml();
        // P1-132: fixed ingestion.json would corrupt across writers.
        assertFalse(xml.contains("ingestion.json\""),
                "fixed ingestion.json filename must not come back");
        assertTrue(xml.contains("ingestion-${HOST}-${VM_ID}.json"),
                "JSON log filename must carry HOST+VM_ID identity");
    }

    @Test
    void environmentValueFollowsCanonicalKey() throws Exception {
        String xml = log4j2Xml();
        // P1-077: the `environment` value must read DEPLOYMENT_ENV first
        // (DEPLOY_ENV alias), same rule as IngestionConfig.
        assertTrue(xml.contains("${env:DEPLOYMENT_ENV:-${env:DEPLOY_ENV:-dev}}"),
                "ENV property must prefer the canonical DEPLOYMENT_ENV key");
    }

    @Test
    void contractFieldNameIsEnvironmentNotEnv() throws Exception {
        String xml = log4j2Xml();
        // P1-134: contract §F requires `environment`.
        assertTrue(xml.contains("key=\"environment\""),
                "log must emit the contract's `environment` key");
        assertFalse(xml.contains("key=\"env\""),
                "short `env` key must not come back");
    }
}
