/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#[cfg(test)]
mod metrics_test {
    use crate::integration::utils::get_shared_cluster;
    use fluss::metadata::TablePath;
    use fluss::metrics::{CLIENT_METADATA_ERRORS_TOTAL, CLIENT_METADATA_REFRESHES_TOTAL};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn counter_value(
        entries: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
    ) -> u64 {
        entries
            .iter()
            .find_map(|(key, _, _, value)| {
                if key.key().name() != name {
                    return None;
                }
                match value {
                    DebugValue::Counter(v) => Some(*v),
                    _ => None,
                }
            })
            .unwrap_or(0)
    }

    #[test]
    fn metadata_refresh_error_metrics_increment_for_failed_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        let before = snapshotter.snapshot().into_vec();
        let refresh_before = counter_value(&before, CLIENT_METADATA_REFRESHES_TOTAL);
        let errors_before = counter_value(&before, CLIENT_METADATA_ERRORS_TOTAL);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        metrics::with_local_recorder(&recorder, || {
            rt.block_on(async {
                let cluster = get_shared_cluster();
                let connection = cluster.get_fluss_connection().await;
                let missing_table = TablePath::new("fluss", "metrics_missing_table");
                let result = connection
                    .get_metadata()
                    .update_table_metadata(&missing_table)
                    .await;
                assert!(result.is_err(), "missing-table metadata update should fail");
            })
        });

        let after = snapshotter.snapshot().into_vec();
        let refresh_after = counter_value(&after, CLIENT_METADATA_REFRESHES_TOTAL);
        let errors_after = counter_value(&after, CLIENT_METADATA_ERRORS_TOTAL);

        assert_eq!(refresh_after - refresh_before, 1);
        assert_eq!(errors_after - errors_before, 1);
    }
}
