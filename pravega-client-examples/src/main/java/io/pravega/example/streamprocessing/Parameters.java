package io.pravega.example.streamprocessing;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Kubernetes, etc.
class Parameters {
    // By default, we will connect to a standalone Pravega running on localhost.
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER", "tcp://localhost:9090"));
    }

    public static String getScope() {
        return getEnvVar("PRAVEGA_SCOPE", "examples");
    }

    public static String getStream1Name() {
        return getEnvVar("PRAVEGA_STREAM_1", "streamprocessing1");
    }

    public static String getStream2Name() {
        return getEnvVar("PRAVEGA_STREAM_2", "streamprocessing2");
    }

    public static int getTargetRateEventsPerSec() {
        return Integer.parseInt(getEnvVar("PRAVEGA_TARGET_RATE_EVENTS_PER_SEC", "10"));
    }

    public static int getScaleFactor() {
        return Integer.parseInt(getEnvVar("PRAVEGA_SCALE_FACTOR", "2"));
    }

    public static int getMinNumSegments() {
        return Integer.parseInt(getEnvVar("PRAVEGA_MIN_NUM_SEGMENTS", "3"));
    }

    public static int getNumWorkers() {
        return Integer.parseInt(getEnvVar("NUM_WORKERS", "2"));
    }

    public static Path getCheckpointRootPath() {
        return Paths.get(getEnvVar("CHECKPOINT_ROOT_PATH", "/tmp/checkpoints"));
    }

    public static long getCheckpointPeriodMs() {
        return Long.parseLong(getEnvVar("CHECKPOINT_PERIOD_MS", "3000"));
    }

    public static long getCheckpointTimeoutMs() {
        return Long.parseLong(getEnvVar("CHECKPOINT_TIMEOUT_MS", "120000"));
    }

    public static long getTransactionTimeoutMs() {
        return Long.parseLong(getEnvVar("TRANSACTION_TIMEOUT_MS", "120000"));
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
