package io.pravega.example.simple_grpc_server;

import java.net.URI;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Mesos, Kubernetes, etc.
class Parameters {
    // By default, we will connect to a standalone Pravega running on localhost.
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER", "tcp://localhost:9090"));
    }

//    public static String getScope() {
//        return getEnvVar("PRAVEGA_SCOPE", "taxidemo");
//    }
//
//    public static String getStreamName() {
//        return getEnvVar("PRAVEGA_STREAM", "data");
//    }
//
//    public static int getTargetRateEventsPerSec() {
//        return Integer.parseInt(getEnvVar("PRAVEGA_TARGET_RATE_EVENTS_PER_SEC", "100"));
//    }
//
//    public static int getScaleFactor() {
//        return Integer.parseInt(getEnvVar("PRAVEGA_SCALE_FACTOR", "2"));
//    }
//
//    public static int getMinNumSegments() {
//        return Integer.parseInt(getEnvVar("PRAVEGA_MIN_NUM_SEGMENTS", "1"));
//    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
