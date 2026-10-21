use pyo3::prelude::*;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use reqwest::Client as ReqwestClient;
use futures::TryStreamExt;

pub const MAX_S3_ATTEMPTS: u32 = 5;
pub const INITIAL_BACKOFF_MS: u64 = 500;
pub const MAX_CONCURRENCY: usize = 32;

/// S3 Response Classification for resilient retry decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum S3ResponseClassification {
    /// S3/OceanStor confirmed that the object does not exist (NoSuchKey)
    ObjectNotFound,
    /// Transient network or server error to retry (TCP reset, broken pipe, unexpected EOF, 5xx, 429, timeout)
    TransientRetryable,
    /// Critical fatal error: proxy 404 without NoSuchKey, NoSuchBucket, AccessDenied
    CriticalFatal(String),
}

/// Classifies errors returned by GET / range download requests.
pub fn classify_get_error<E: std::fmt::Debug>(err: &E) -> S3ResponseClassification {
    let s = format!("{:?}", err);
    if s.contains("NoSuchBucket") {
        return S3ResponseClassification::CriticalFatal("Bucket does not exist (NoSuchBucket)".to_string());
    }
    if s.contains("NoSuchKey") {
        return S3ResponseClassification::ObjectNotFound;
    }
    if s.contains("404") || s.contains("NotFound") {
        return S3ResponseClassification::CriticalFatal(format!(
            "Received 404 without S3 NoSuchKey (possible reverse proxy / ingress routing failure): {}", s
        ));
    }
    if s.contains("AccessDenied") || s.contains("403") {
        return S3ResponseClassification::CriticalFatal(format!("Access Denied: {}", s));
    }
    S3ResponseClassification::TransientRetryable
}

/// Classifies errors returned by HEAD existence checks.
pub fn classify_head_error<E: std::fmt::Debug>(err: &E) -> S3ResponseClassification {
    let s = format!("{:?}", err);
    if s.contains("NoSuchBucket") {
        return S3ResponseClassification::CriticalFatal("Bucket does not exist (NoSuchBucket)".to_string());
    }
    if s.contains("NotFound") || s.contains("NoSuchKey") || s.contains("404") {
        // Valid S3 HEAD response contains SDK NotFound marker, NoSuchKey, or S3 request tracking headers
        if s.contains("x-amz-request-id") || s.contains("NotFound") || s.contains("NoSuchKey") {
            return S3ResponseClassification::ObjectNotFound;
        }
        return S3ResponseClassification::CriticalFatal(format!(
            "Received raw 404 on HEAD without S3 markers (possible reverse proxy error): {}", s
        ));
    }
    if s.contains("AccessDenied") || s.contains("403") {
        return S3ResponseClassification::CriticalFatal(format!("Access Denied: {}", s));
    }
    S3ResponseClassification::TransientRetryable
}

/// Calculates exponential backoff with non-deterministic jitter to avoid thundering herd.
pub fn calculate_backoff(attempt: u32) -> std::time::Duration {
    let shift = attempt.saturating_sub(1).min(6);
    let base_ms = INITIAL_BACKOFF_MS.saturating_mul(1 << shift);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter_ms = (nanos % 250) as u64;
    std::time::Duration::from_millis(base_ms + jitter_ms)
}

/// Builds an AWS SDK S3 Client configured for resilient operation with custom timeouts.
pub fn create_s3_client() -> PyResult<(Client, String)> {
    let endpoint = std::env::var("XET_CAS_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key = std::env::var("XET_CAS_S3_API_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = std::env::var("XET_CAS_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let region = std::env::var("XET_CAS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let bucket = std::env::var("XET_CAS_S3_BUCKET_NAME").unwrap_or_else(|_| "xet-cas".to_string());

    let credentials = Credentials::new(access_key, secret_key, None, None, "xet_janitor");
    // Single internal attempt so our unified outer retry manages backoff, jitter, and classification
    let retry_config = aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1);
    let timeout_config = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(60))
        .build();

    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .retry_config(retry_config)
        .timeout_config(timeout_config)
        .credentials_provider(credentials)
        .region(aws_config::Region::new(region))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();

    Ok((Client::from_conf(config), bucket))
}

// ============================================================================
// AWS SDK S3 Resilient Helpers
// ============================================================================

/// Downloads a full object, recovering from mid-flight stream dropouts and returning immediately on 404.
pub async fn s3_download_object(
    client: &Client,
    bucket: &str,
    key: &str,
    max_attempts: u32,
    must_exist: bool,
) -> Result<Option<Vec<u8>>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(resp) => {
                let content_len = resp.content_length;
                match resp.body.collect().await {
                    Ok(collected) => {
                        let bytes = collected.into_bytes().to_vec();
                        if let Some(expected) = content_len {
                            if bytes.len() as i64 != expected {
                                if attempts >= max_attempts {
                                    return Err(format!("Stream truncated for {}: expected {} bytes, got {}", key, expected, bytes.len()));
                                }
                                eprintln!("[S3 Get] Stream truncated mid-flight for {} (got {}/{}), retrying...", key, bytes.len(), expected);
                                tokio::time::sleep(calculate_backoff(attempts)).await;
                                continue;
                            }
                        }
                        return Ok(Some(bytes));
                    }
                    Err(e) => {
                        if attempts >= max_attempts {
                            return Err(format!("Stream aborted mid-flight after {} attempts for {}: {:?}", max_attempts, key, e));
                        }
                        eprintln!("[S3 Get] Connection dropped mid-stream for {}: {:?}. Retrying...", key, e);
                        tokio::time::sleep(calculate_backoff(attempts)).await;
                        continue;
                    }
                }
            }
            Err(e) => {
                match classify_get_error(&e) {
                    S3ResponseClassification::ObjectNotFound => {
                        if must_exist {
                            return Err(format!("Expected object {} does not exist in S3 (NoSuchKey confirmed)", key));
                        }
                        return Ok(None);
                    }
                    S3ResponseClassification::TransientRetryable => {
                        if attempts >= max_attempts {
                            return Err(format!("Exhausted {} attempts getting {}: {:?}", max_attempts, key, e));
                        }
                        tokio::time::sleep(calculate_backoff(attempts)).await;
                    }
                    S3ResponseClassification::CriticalFatal(msg) => {
                        return Err(format!("Critical fatal GET error for {}: {}", key, msg));
                    }
                }
            }
        }
    }
}

/// Downloads a byte range with mid-stream disconnect recovery and missing-data safety guards.
pub async fn s3_download_range(
    client: &Client,
    bucket: &str,
    key: &str,
    range_header: &str,
    max_attempts: u32,
    must_exist: bool,
) -> Result<Option<Vec<u8>>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.get_object().bucket(bucket).key(key).range(range_header).send().await {
            Ok(resp) => {
                match resp.body.collect().await {
                    Ok(collected) => return Ok(Some(collected.into_bytes().to_vec())),
                    Err(e) => {
                        if attempts >= max_attempts {
                            return Err(format!("Stream aborted mid-flight for range {} on {}: {:?}", range_header, key, e));
                        }
                        eprintln!("[S3 Range] Connection dropped mid-stream for {} ({}): {:?}. Retrying...", key, range_header, e);
                        tokio::time::sleep(calculate_backoff(attempts)).await;
                        continue;
                    }
                }
            }
            Err(e) => {
                match classify_get_error(&e) {
                    S3ResponseClassification::ObjectNotFound => {
                        if must_exist {
                            return Err(format!("Expected object {} (range {}) does not exist in S3 (NoSuchKey confirmed)", key, range_header));
                        }
                        return Ok(None);
                    }
                    S3ResponseClassification::TransientRetryable => {
                        if attempts >= max_attempts {
                            return Err(format!("Exhausted {} attempts fetching range {} for {}: {:?}", max_attempts, range_header, key, e));
                        }
                        tokio::time::sleep(calculate_backoff(attempts)).await;
                    }
                    S3ResponseClassification::CriticalFatal(msg) => {
                        return Err(format!("Critical fatal range error for {}: {}", key, msg));
                    }
                }
            }
        }
    }
}

/// Checks object existence via HEAD with retry on 404s and network drops. Never swallows network errors.
pub async fn s3_check_exists(
    client: &Client,
    bucket: &str,
    key: &str,
    max_attempts: u32,
) -> Result<bool, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.head_object().bucket(bucket).key(key).send().await {
            Ok(_) => return Ok(true),
            Err(e) => {
                match classify_head_error(&e) {
                    S3ResponseClassification::ObjectNotFound => {
                        return Ok(false);
                    }
                    S3ResponseClassification::TransientRetryable => {
                        if attempts >= max_attempts {
                            return Err(format!("Exhausted {} retries checking existence of {}: {:?}", max_attempts, key, e));
                        }
                        eprintln!("[S3 Exists] Network error checking {} (attempt {}/{}). Retrying...", key, attempts, max_attempts);
                        tokio::time::sleep(calculate_backoff(attempts)).await;
                    }
                    S3ResponseClassification::CriticalFatal(msg) => {
                        return Err(format!("Critical fatal HEAD error checking {}: {}", key, msg));
                    }
                }
            }
        }
    }
}

/// Puts an object with zero-copy stream replay to safely handle mid-flight socket drops.
pub async fn s3_put_object(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Vec<u8>,
    max_attempts: u32,
) -> Result<(), String> {
    let mut attempts = 0;
    let data_bytes = bytes::Bytes::from(data);
    loop {
        attempts += 1;
        let body = ByteStream::from(data_bytes.clone());
        match client.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await 
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("NoSuchBucket") || err_str.contains("AccessDenied") {
                    return Err(format!("Fatal S3 PUT error for {}: {}", key, err_str));
                }
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} attempts putting {}: {}", max_attempts, key, err_str));
                }
                eprintln!("[S3 Put] Upload failed mid-flight for {} (attempt {}/{}): {}. Retrying...", key, attempts, max_attempts, err_str);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Copies an S3 object with retries. Optionally ignores 404 if relocating already-moved items.
pub async fn s3_copy_object(
    client: &Client,
    bucket: &str,
    src: &str,
    dest: &str,
    max_attempts: u32,
    ignore_not_found: bool,
) -> Result<(), String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.copy_object().copy_source(src).bucket(bucket).key(dest).send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let err_str = format!("{:?}", e);
                if ignore_not_found && (err_str.contains("NotFound") || err_str.contains("NoSuchKey") || err_str.contains("404")) {
                    return Ok(());
                }
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} retries copying {} to {}: {}", max_attempts, src, dest, err_str));
                }
                eprintln!("[S3 Copy] Copy failed for {} -> {} (attempt {}/{}). Retrying...", src, dest, attempts, max_attempts);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Deletes an S3 object with retries. Treats NoSuchKey/404 as successful idempotent deletion.
pub async fn s3_delete_object(
    client: &Client,
    bucket: &str,
    key: &str,
    max_attempts: u32,
) -> Result<(), String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.delete_object().bucket(bucket).key(key).send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("NotFound") || err_str.contains("NoSuchKey") || err_str.contains("404") {
                    return Ok(());
                }
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} retries deleting {}: {}", max_attempts, key, err_str));
                }
                eprintln!("[S3 Delete] Delete failed for {} (attempt {}/{}). Retrying...", key, attempts, max_attempts);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Lists objects with retry to prevent premature abort of GC cleanup loops.
pub async fn s3_list_objects_v2(
    client: &Client,
    bucket: &str,
    prefix: &str,
    continuation_token: Option<&str>,
    max_attempts: u32,
) -> Result<ListObjectsV2Output, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }
        match req.send().await {
            Ok(output) => return Ok(output),
            Err(e) => {
                let err_str = format!("{:?}", e);
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} retries listing objects with prefix {}: {}", max_attempts, prefix, err_str));
                }
                eprintln!("[S3 List] List failed for prefix {} (attempt {}/{}). Retrying...", prefix, attempts, max_attempts);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Uploads large transaction files via Reqwest HTTP/1.1 with exponential backoff.
pub async fn s3_upload_file_reqwest_http1(
    client: &Client,
    bucket: &str,
    key: &str,
    file_path: &str,
    max_attempts: u32,
) -> Result<(), String> {
    let presigned = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .presigned(aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(600)).map_err(|e| format!("{:?}", e))?)
        .await
        .map_err(|e| format!("Presigning failed for {}: {:?}", key, e))?;
        
    let reqwest_client = ReqwestClient::builder()
        .http1_only()
        .danger_accept_invalid_certs(true)
        .build()
        .map_err(|e| format!("Reqwest init failed: {:?}", e))?;
        
    let file_data = tokio::fs::read(file_path).await.map_err(|e| format!("Failed to read file {file_path}: {:?}", e))?;
    let size = file_data.len();
    
    let mut attempts = 0;
    loop {
        attempts += 1;
        eprintln!("[GC Upload] Uploading {} ({:.2} MB, attempt {}/{})...", key, size as f64 / (1024.0 * 1024.0), attempts, max_attempts);
        let body = reqwest::Body::from(file_data.clone());
        
        let request = reqwest_client.put(presigned.uri().to_string())
            .header("Content-Length", size.to_string())
            .body(body);
            
        match request.send().await {
            Ok(r) if r.status().is_success() => {
                eprintln!("[GC Upload] Successfully uploaded {} ({} bytes)", key, size);
                return Ok(());
            }
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                let err = format!("HTTP {}: {}", status, text);
                eprintln!("[GC Upload] Attempt {} failed for {}: {}", attempts, key, err);
                if attempts >= max_attempts {
                    return Err(format!("Failed to put {}: {}", key, err));
                }
            }
            Err(e) => {
                eprintln!("[GC Upload] Attempt {} network error for {}: {:?}", attempts, key, e);
                if attempts >= max_attempts {
                    return Err(format!("Reqwest error putting {}: {:?}", key, e));
                }
            }
        }
        tokio::time::sleep(calculate_backoff(attempts)).await;
    }
}

// ============================================================================
// Reqwest Helpers for Presigned URLs (Used by lib.rs)
// ============================================================================

/// Downloads XORB footer via presigned URL with retries on mid-stream drops.
pub async fn reqwest_download_footer_with_retry(
    client: &ReqwestClient,
    url: &str,
    xorb_hash_hex: &str,
    max_attempts: u32,
) -> Result<Option<Vec<u8>>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let resp_res = client.get(url)
            .header("Range", "bytes=-1048576")
            .send()
            .await;

        match resp_res {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT {
                    match resp.bytes().await {
                        Ok(b) => return Ok(Some(b.to_vec())),
                        Err(e) => {
                            if attempts >= max_attempts {
                                return Err(format!("Stream aborted reading footer for {} after {} attempts: {:?}", xorb_hash_hex, max_attempts, e));
                            }
                            eprintln!("[Reqwest] Footer stream dropped for {} (attempt {}/{}). Retrying...", xorb_hash_hex, attempts, max_attempts);
                            tokio::time::sleep(calculate_backoff(attempts)).await;
                            continue;
                        }
                    }
                } else if status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                } else {
                    let err = format!("HTTP {}", status);
                    if attempts >= max_attempts {
                        return Err(format!("Failed to fetch footer for {}: {}", xorb_hash_hex, err));
                    }
                    eprintln!("[Reqwest] Footer fetch for {} failed with {} (attempt {}/{}). Retrying...", xorb_hash_hex, status, attempts, max_attempts);
                    tokio::time::sleep(calculate_backoff(attempts)).await;
                }
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} attempts fetching footer for {}: {:?}", max_attempts, xorb_hash_hex, e));
                }
                eprintln!("[Reqwest] Network error fetching footer for {} (attempt {}/{}): {:?}. Retrying...", xorb_hash_hex, attempts, max_attempts, e);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Downloads and decompresses a range of chunks, retrying mid-flight stream dropouts from byte 0.
pub async fn reqwest_download_and_decompress_range_with_retry(
    client: &ReqwestClient,
    url: &str,
    b_start: u64,
    b_end: u64,
    unpacked_size: u32,
    max_attempts: u32,
) -> Result<Vec<u8>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        let range_header = format!("bytes={}-{}", b_start, b_end);
        let resp_res = client.get(url)
            .header("Range", &range_header)
            .send()
            .await;

        match resp_res {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
                    let err = format!("HTTP {}", status);
                    if attempts >= max_attempts {
                        return Err(format!("Fetch range {} failed for {}: {}", range_header, url, err));
                    }
                    eprintln!("[Reqwest Range] HTTP {} for range {} (attempt {}/{}). Retrying...", status, range_header, attempts, max_attempts);
                    tokio::time::sleep(calculate_backoff(attempts)).await;
                    continue;
                }

                let mut decompressed = Vec::new();
                let mut writer = std::io::Cursor::new(&mut decompressed);

                let mut stream_reader = resp.bytes_stream()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                    .into_async_read();

                let mut total_unpacked = 0u32;
                let mut decompress_failed = false;

                while total_unpacked < unpacked_size {
                    match mdb_shard::xorb_object::deserialize_async::deserialize_chunk_to_writer(&mut stream_reader, &mut writer).await {
                        Ok((_, unpacked_len)) => {
                            total_unpacked += unpacked_len;
                        }
                        Err(e) => {
                            decompress_failed = true;
                            if attempts >= max_attempts {
                                return Err(format!("Decompression failed for range {} on {} after {} attempts: {:?}", range_header, url, max_attempts, e));
                            }
                            eprintln!("[Reqwest Range] Stream dropped/decompression error for {} range {} (attempt {}/{}): {:?}. Retrying from byte 0...", url, range_header, attempts, max_attempts, e);
                            break;
                        }
                    }
                }

                if !decompress_failed {
                    return Ok(decompressed);
                }

                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(format!("Exhausted {} attempts fetching range {} for {}: {:?}", max_attempts, range_header, url, e));
                }
                eprintln!("[Reqwest Range] Network error fetching range {} for {} (attempt {}/{}): {:?}. Retrying...", range_header, url, attempts, max_attempts, e);
                tokio::time::sleep(calculate_backoff(attempts)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_get_error() {
        // Genuine NoSuchKey is classified as ObjectNotFound
        let err_no_such_key = "ServiceError(GetObjectError { kind: NoSuchKey })";
        assert_eq!(classify_get_error(&err_no_such_key), S3ResponseClassification::ObjectNotFound);

        // Missing bucket is a fatal error
        let err_no_such_bucket = "ServiceError(GetObjectError { kind: NoSuchBucket })";
        match classify_get_error(&err_no_such_bucket) {
            S3ResponseClassification::CriticalFatal(msg) => assert!(msg.contains("NoSuchBucket")),
            other => panic!("Expected CriticalFatal, got {:?}", other),
        }

        // Generic 404 without NoSuchKey is a fatal reverse proxy error
        let err_proxy_404 = "ResponseError { status: 404, message: \"nginx 404 Not Found\" }";
        match classify_get_error(&err_proxy_404) {
            S3ResponseClassification::CriticalFatal(msg) => assert!(msg.contains("reverse proxy")),
            other => panic!("Expected CriticalFatal, got {:?}", other),
        }

        // AccessDenied / 403 is a fatal error
        let err_403 = "ServiceError(GetObjectError { kind: AccessDenied, status: 403 })";
        match classify_get_error(&err_403) {
            S3ResponseClassification::CriticalFatal(msg) => assert!(msg.contains("Access Denied")),
            other => panic!("Expected CriticalFatal, got {:?}", other),
        }

        // Connection reset / timeout is transient retryable
        let err_reset = "DispatchFailure(ConnectorError { source: Connection reset by peer (os error 104) })";
        assert_eq!(classify_get_error(&err_reset), S3ResponseClassification::TransientRetryable);
    }

    #[test]
    fn test_classify_head_error() {
        // Genuine HEAD 404 with x-amz-request-id or NotFound
        let err_head_404_s3 = "HeadObjectError::NotFound: x-amz-request-id: 123456789";
        assert_eq!(classify_head_error(&err_head_404_s3), S3ResponseClassification::ObjectNotFound);

        // Raw 404 without S3 headers is fatal
        let err_raw_404 = "HTTP 404 Not Found from gateway";
        match classify_head_error(&err_raw_404) {
            S3ResponseClassification::CriticalFatal(msg) => assert!(msg.contains("reverse proxy")),
            other => panic!("Expected CriticalFatal, got {:?}", other),
        }

        // Transient timeout
        let err_timeout = "DispatchFailure(TimedOut)";
        assert_eq!(classify_head_error(&err_timeout), S3ResponseClassification::TransientRetryable);
    }

    #[test]
    fn test_calculate_backoff() {
        let b1 = calculate_backoff(1);
        let b2 = calculate_backoff(2);
        let b3 = calculate_backoff(3);

        assert!(b1.as_millis() >= 500 && b1.as_millis() < 800);
        assert!(b2.as_millis() >= 1000 && b2.as_millis() < 1300);
        assert!(b3.as_millis() >= 2000 && b3.as_millis() < 2300);
    }
}

