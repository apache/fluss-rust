use crate::{impl_read_version_type, impl_write_version_type, proto};

use crate::error::Result as FlussResult;
use crate::proto::ListOffsetsResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use futures::future::join_all;
use std::collections::HashMap;
use tokio::sync::{oneshot};

use bytes::{Buf, BufMut};
use prost::Message;

/// Offset type constants as per proto comments
pub const LIST_EARLIEST_OFFSET: i32 = 0;
pub const LIST_LATEST_OFFSET: i32 = 1;
pub const LIST_OFFSET_FROM_TIMESTAMP: i32 = 2;

/// Client follower server id constant
pub const CLIENT_FOLLOWER_SERVER_ID: i32 = -1;

/// Offset specification for list offsets request
#[derive(Debug, Clone)]
pub enum OffsetSpec {
    /// Earliest offset spec
    Earliest,
    /// Latest offset spec  
    Latest,
    /// Timestamp offset spec
    Timestamp(i64),
}

impl OffsetSpec {
    pub fn offset_type(&self) -> i32 {
        match self {
            OffsetSpec::Earliest => LIST_EARLIEST_OFFSET,
            OffsetSpec::Latest => LIST_LATEST_OFFSET,
            OffsetSpec::Timestamp(_) => LIST_OFFSET_FROM_TIMESTAMP,
        }
    }

    pub fn start_timestamp(&self) -> Option<i64> {
        match self {
            OffsetSpec::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }
}

/// Result container for list offsets operation
pub struct ListOffsetsResult {
    futures: HashMap<i32, oneshot::Receiver<FlussResult<i64>>>,
}

impl ListOffsetsResult {
    pub fn new(futures: HashMap<i32, oneshot::Receiver<FlussResult<i64>>>) -> Self {
        Self { futures }
    }
    
    /// Get the offset result for a specific bucket
    pub async fn bucket_result(&mut self, bucket: i32) -> FlussResult<i64> {
        if let Some(receiver) = self.futures.remove(&bucket) {
            receiver.await
                .map_err(|_| crate::error::Error::WriteError("Channel closed".to_string()))?
        } else {
            Err(crate::error::Error::IllegalArgument(
                format!("Bucket {} not found", bucket)
            ))
        }
    }
    
    /// Wait for all bucket results to complete and return a map
    pub async fn all(self) -> FlussResult<HashMap<i32, i64>> {
        let mut results = HashMap::new();
        let mut tasks = Vec::new();
        
        // Collect all futures
        for (bucket_id, receiver) in self.futures {
            let task = async move {
                let result = receiver.await
                    .map_err(|_| crate::error::Error::WriteError("Channel closed".to_string()))?;
                Ok::<(i32, i64), crate::error::Error>((bucket_id, result?))
            };
            tasks.push(task);
        }
        
        // Wait for all futures to complete
        let task_results = join_all(tasks).await;
        
        // Collect results
        for task_result in task_results {
            let (bucket_id, offset) = task_result?;
            results.insert(bucket_id, offset);
        }
        
        Ok(results)
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    pub inner_request: proto::ListOffsetsRequest,
}

impl ListOffsetsRequest {
    pub fn new(
        table_id: i64,
        partition_id: Option<i64>,
        bucket_ids: Vec<i32>,
        offset_spec: OffsetSpec,
    ) -> FlussResult<Self> {
        Ok(ListOffsetsRequest {
            inner_request: proto::ListOffsetsRequest {
                follower_server_id: CLIENT_FOLLOWER_SERVER_ID,
                offset_type: offset_spec.offset_type(),
                table_id,
                partition_id,
                bucket_id: bucket_ids,
                start_timestamp: offset_spec.start_timestamp(),
            },
        })
    }
}

impl RequestBody for ListOffsetsRequest {
    type ResponseBody = ListOffsetsResponse;

    const API_KEY: ApiKey = ApiKey::ListOffsets;

    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListOffsetsRequest);
impl_read_version_type!(ListOffsetsResponse);