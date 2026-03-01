// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Metric name constants and helpers for fluss-rust client instrumentation.
//!
//! Uses the [`metrics`] crate facade pattern: library code emits metrics via
//! `counter!`/`gauge!`/`histogram!` macros, and the application installs a
//! recorder (e.g. `metrics-exporter-prometheus`) to collect them. When no
//! recorder is installed, all metric calls are no-ops with zero overhead.

use crate::rpc::ApiKey;

// ---------------------------------------------------------------------------
// Label keys
// ---------------------------------------------------------------------------

pub const LABEL_API_KEY: &str = "api_key";

// ---------------------------------------------------------------------------
// Connection / RPC metrics
//
// Java reference: ConnectionMetrics.java, ClientMetricGroup.java, MetricNames.java
//
// Note on bytes_received: Rust counts the response body length (after the
// ResponseHeader). Java uses ApiMessage.totalSize() which may include framing.
// Absolute values can differ slightly; the semantic (bytes received per
// response) is the same.
// ---------------------------------------------------------------------------

pub const CLIENT_REQUESTS_TOTAL: &str = "fluss.client.requests.total";
pub const CLIENT_RESPONSES_TOTAL: &str = "fluss.client.responses.total";
pub const CLIENT_BYTES_SENT_TOTAL: &str = "fluss.client.bytes_sent.total";
pub const CLIENT_BYTES_RECEIVED_TOTAL: &str = "fluss.client.bytes_received.total";
pub const CLIENT_REQUEST_LATENCY_MS: &str = "fluss.client.request_latency_ms";
pub const CLIENT_REQUESTS_IN_FLIGHT: &str = "fluss.client.requests_in_flight";

/// Returns a label value for reportable API keys, matching Java's
/// `ConnectionMetrics.REPORT_API_KEYS` filter (`ProduceLog`, `FetchLog`,
/// `PutKv`, `Lookup`). Returns `None` for admin/metadata/auth calls to
/// avoid metric cardinality bloat.
pub(crate) fn api_key_label(api_key: ApiKey) -> Option<&'static str> {
    match api_key {
        ApiKey::ProduceLog => Some("produce_log"),
        ApiKey::FetchLog => Some("fetch_log"),
        ApiKey::PutKv => Some("put_kv"),
        ApiKey::Lookup => Some("lookup"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::DebuggingRecorder;

    #[test]
    fn reportable_api_keys_return_label() {
        assert_eq!(api_key_label(ApiKey::ProduceLog), Some("produce_log"));
        assert_eq!(api_key_label(ApiKey::FetchLog), Some("fetch_log"));
        assert_eq!(api_key_label(ApiKey::PutKv), Some("put_kv"));
        assert_eq!(api_key_label(ApiKey::Lookup), Some("lookup"));
    }

    #[test]
    fn non_reportable_api_keys_return_none() {
        assert_eq!(api_key_label(ApiKey::MetaData), None);
        assert_eq!(api_key_label(ApiKey::CreateTable), None);
        assert_eq!(api_key_label(ApiKey::Authenticate), None);
        assert_eq!(api_key_label(ApiKey::ListDatabases), None);
        assert_eq!(api_key_label(ApiKey::GetTable), None);
    }

    #[test]
    fn reportable_request_records_all_connection_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::ProduceLog).unwrap();

            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => label).increment(1);
            metrics::counter!(CLIENT_BYTES_SENT_TOTAL, LABEL_API_KEY => label).increment(256);
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).increment(1.0);

            metrics::counter!(CLIENT_RESPONSES_TOTAL, LABEL_API_KEY => label).increment(1);
            metrics::counter!(CLIENT_BYTES_RECEIVED_TOTAL, LABEL_API_KEY => label).increment(128);
            metrics::histogram!(CLIENT_REQUEST_LATENCY_MS, LABEL_API_KEY => label).record(42.5);
            metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        let find_counter = |name: &str| -> Option<u64> {
            entries.iter().find_map(|(key, _, _, val)| {
                if key.key().name() == name {
                    match val {
                        metrics_util::debugging::DebugValue::Counter(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        };

        let find_histogram = |name: &str| -> Option<Vec<f64>> {
            entries.iter().find_map(|(key, _, _, val)| {
                if key.key().name() == name {
                    match val {
                        metrics_util::debugging::DebugValue::Histogram(v) => {
                            Some(v.iter().map(|f| f.into_inner()).collect())
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            })
        };

        assert_eq!(find_counter(CLIENT_REQUESTS_TOTAL), Some(1));
        assert_eq!(find_counter(CLIENT_RESPONSES_TOTAL), Some(1));
        assert_eq!(find_counter(CLIENT_BYTES_SENT_TOTAL), Some(256));
        assert_eq!(find_counter(CLIENT_BYTES_RECEIVED_TOTAL), Some(128));
        assert_eq!(find_histogram(CLIENT_REQUEST_LATENCY_MS), Some(vec![42.5]));

        let has_label = entries.iter().all(|(key, _, _, _)| {
            key.key()
                .labels()
                .any(|l| l.key() == LABEL_API_KEY && l.value() == "produce_log")
        });
        assert!(has_label, "all metrics must carry the api_key label");
    }

    #[test]
    fn non_reportable_request_records_no_metrics() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::MetaData);
            assert!(label.is_none());
            // When label is None, no metrics calls are made (matching request() logic).
        });

        let snapshot = snapshotter.snapshot();
        assert!(
            snapshot.into_vec().is_empty(),
            "non-reportable API keys must not produce metrics"
        );
    }

    #[test]
    fn inflight_gauge_nets_to_zero_after_balanced_calls() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let label = api_key_label(ApiKey::FetchLog).unwrap();

            // Simulate 3 concurrent requests completing
            for _ in 0..3 {
                metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).increment(1.0);
            }
            for _ in 0..3 {
                metrics::gauge!(CLIENT_REQUESTS_IN_FLIGHT, LABEL_API_KEY => label).decrement(1.0);
            }
        });

        let snapshot = snapshotter.snapshot();
        for (key, _, _, val) in snapshot.into_vec() {
            if key.key().name() == CLIENT_REQUESTS_IN_FLIGHT {
                match val {
                    metrics_util::debugging::DebugValue::Gauge(g) => {
                        let value: f64 = g.into_inner();
                        assert!(
                            value == 0.0,
                            "in-flight gauge should be 0 after balanced inc/dec, got: {value}"
                        );
                    }
                    other => panic!("expected Gauge, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn different_api_keys_produce_separate_metric_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let produce_label = api_key_label(ApiKey::ProduceLog).unwrap();
            let fetch_label = api_key_label(ApiKey::FetchLog).unwrap();

            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => produce_label).increment(5);
            metrics::counter!(CLIENT_REQUESTS_TOTAL, LABEL_API_KEY => fetch_label).increment(3);
        });

        let snapshot = snapshotter.snapshot();
        let entries: Vec<_> = snapshot.into_vec();

        let request_entries: Vec<_> = entries
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == CLIENT_REQUESTS_TOTAL)
            .collect();

        assert_eq!(
            request_entries.len(),
            2,
            "produce_log and fetch_log should be separate metric series"
        );
    }
}
