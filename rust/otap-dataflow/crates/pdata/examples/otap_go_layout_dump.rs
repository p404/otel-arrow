// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Diagnosis rig: serve the OTAP logs Arrow stream and dump the exact Arrow
//! layout a Go `otelarrow` exporter puts on the wire, before and after
//! `decode_transport_optimized_ids`, alongside the OTLP the encoder produces.
//!
//!     cargo run -p otap-df-pdata --example otap_go_layout_dump

// Printing the decoded layout is the whole point of this binary, and the
// server task is detached for the lifetime of the process.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::let_underscore_future
)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::util::pretty::pretty_format_batches;
use otap_df_pdata::Consumer;
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otap::{Logs, OtapArrowRecords, from_record_messages};
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::proto::opentelemetry::arrow::v1::{
    BatchArrowRecords, BatchStatus, StatusCode,
    arrow_logs_service_server::{ArrowLogsService, ArrowLogsServiceServer},
};
use otap_df_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

type ResponseStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<BatchStatus, Status>> + Send + 'static>>;

#[derive(Default)]
struct DumpService {
    seq: Arc<AtomicUsize>,
}

fn dump_batch(seq: usize, otap: &mut OtapArrowRecords) {
    println!("\n================ BATCH {seq} ================");
    for payload_type in otap.allowed_payload_types().iter().copied() {
        if let Some(rb) = otap.get(payload_type) {
            println!("--- payload {payload_type:?} rows={} ---", rb.num_rows());
            println!("schema: {:#?}", rb.schema());
            match pretty_format_batches(std::slice::from_ref(rb)) {
                Ok(t) => println!("{t}"),
                Err(e) => println!("(pretty print failed: {e})"),
            }
        }
    }

    let mut decoded = otap.clone();
    match decoded.decode_transport_optimized_ids() {
        Ok(()) => {
            println!("--- AFTER decode_transport_optimized_ids ---");
            for payload_type in decoded.allowed_payload_types().iter().copied() {
                if let Some(rb) = decoded.get(payload_type) {
                    println!(
                        "--- decoded payload {payload_type:?} rows={} ---",
                        rb.num_rows()
                    );
                    match pretty_format_batches(std::slice::from_ref(rb)) {
                        Ok(t) => println!("{t}"),
                        Err(e) => println!("(pretty print failed: {e})"),
                    }
                }
            }
        }
        Err(e) => println!("decode_transport_optimized_ids failed: {e}"),
    }

    let otlp: Result<OtlpProtoBytes, _> = otap.clone().try_into_with_default();
    match otlp {
        Ok(OtlpProtoBytes::ExportLogsRequest(bytes)) => {
            match ExportLogsServiceRequest::decode(&bytes[..]) {
                Ok(req) => {
                    println!("--- OTLP OUT: {} ResourceLogs ---", req.resource_logs.len());
                    for (ri, rl) in req.resource_logs.iter().enumerate() {
                        let attrs: Vec<String> = rl
                            .resource
                            .as_ref()
                            .map(|r| {
                                r.attributes
                                    .iter()
                                    .map(|kv| format!("{}={:?}", kv.key, kv.value))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let n: usize = rl.scope_logs.iter().map(|s| s.log_records.len()).sum();
                        println!("  ResourceLogs[{ri}] attrs={attrs:?} log_records={n}");
                    }
                }
                Err(e) => println!("OTLP decode failed: {e}"),
            }
        }
        Ok(other) => println!("unexpected OTLP variant: {other:?}"),
        Err(e) => println!("OTAP->OTLP encode failed: {e}"),
    }
    println!("================ END BATCH {seq} ================\n");
}

#[tonic::async_trait]
impl ArrowLogsService for DumpService {
    type ArrowLogsStream = ResponseStream;

    async fn arrow_logs(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowLogsStream>, Status> {
        let mut input = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let seq = self.seq.clone();

        let _ = tokio::spawn(async move {
            let mut consumer = Consumer::default();
            while let Ok(Some(mut bar)) = input.message().await {
                let batch_id = bar.batch_id;
                let n = seq.fetch_add(1, Ordering::Relaxed);
                let status = match consumer.consume_bar(&mut bar) {
                    Ok(messages) => match from_record_messages::<Logs>(messages) {
                        Ok(logs) => {
                            let mut otap = OtapArrowRecords::Logs(logs);
                            dump_batch(n, &mut otap);
                            (StatusCode::Ok, "ok".to_string())
                        }
                        Err(e) => (StatusCode::InvalidArgument, format!("assemble: {e}")),
                    },
                    Err(e) => (StatusCode::InvalidArgument, format!("consume: {e}")),
                };
                if tx
                    .send(Ok(BatchStatus {
                        batch_id,
                        status_code: status.0 as i32,
                        status_message: status.1,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)) as Self::ArrowLogsStream,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:14319".parse()?;
    eprintln!("otap_go_layout_dump listening on {addr}");
    Server::builder()
        .add_service(
            ArrowLogsServiceServer::new(DumpService::default())
                .max_decoding_message_size(64 * 1024 * 1024)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
        )
        .serve(addr)
        .await?;
    Ok(())
}
