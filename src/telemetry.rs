//! Shared telemetry bootstrap for CardinalSin binaries.

use crate::{Error, Result};

use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{self, Sampler, TracerProvider};
use opentelemetry_sdk::Resource;
use std::collections::BTreeMap;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

const ATTR_SERVICE_NAME: &str = "service.name";
const ATTR_SERVICE_NAMESPACE: &str = "service.namespace";
const ATTR_CARDINALSIN_RUN_ID: &str = "cardinalsin.run_id";
const OTEL_PROTOCOL_GRPC: &str = "grpc";
const OTEL_PROTOCOL_HTTP_PROTOBUF: &str = "http/protobuf";

/// Runtime mode for telemetry exporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryMode {
    Disabled,
    Otlp,
}

impl TelemetryMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            TelemetryMode::Disabled => "disabled",
            TelemetryMode::Otlp => "otlp",
        }
    }
}

/// Parsed telemetry configuration from environment.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub mode: TelemetryMode,
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub otlp_protocol: String,
    pub traces_sampler: String,
    pub run_id: Option<String>,
    pub resource_attributes: Vec<KeyValue>,
    sampler: Sampler,
}

impl TelemetryConfig {
    pub fn from_env(default_service_name: &str) -> Result<Self> {
        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| default_service_name.to_string());
        let service_name = service_name.trim();
        if service_name.is_empty() {
            return Err(Error::Config(
                "OTEL_SERVICE_NAME cannot be empty".to_string(),
            ));
        }

        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
            .unwrap_or_else(|_| OTEL_PROTOCOL_GRPC.to_string());
        let protocol = parse_otlp_protocol(&protocol)?;

        let telemetry_enabled = parse_optional_bool("CARDINALSIN_TELEMETRY_ENABLED")?;
        let mode = match (telemetry_enabled, endpoint.is_some()) {
            (Some(false), _) => TelemetryMode::Disabled,
            (Some(true), true) => TelemetryMode::Otlp,
            (Some(true), false) => {
                return Err(Error::Config(
                    "CARDINALSIN_TELEMETRY_ENABLED=true requires OTEL_EXPORTER_OTLP_ENDPOINT"
                        .to_string(),
                ));
            }
            (None, true) => TelemetryMode::Otlp,
            (None, false) => TelemetryMode::Disabled,
        };

        let run_id = std::env::var("CARDINALSIN_TELEMETRY_RUN_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let sampler_name = std::env::var("OTEL_TRACES_SAMPLER")
            .unwrap_or_else(|_| "parentbased_always_on".to_string());
        let sampler_arg = std::env::var("OTEL_TRACES_SAMPLER_ARG").ok();
        let sampler = parse_sampler(&sampler_name, sampler_arg.as_deref())?;

        let mut attr_map: BTreeMap<String, String> = BTreeMap::new();
        if let Ok(attr_str) = std::env::var("OTEL_RESOURCE_ATTRIBUTES") {
            for (key, value) in parse_resource_attributes(&attr_str)? {
                attr_map.insert(key, value);
            }
        }

        attr_map.insert(ATTR_SERVICE_NAME.to_string(), service_name.to_string());
        attr_map
            .entry(ATTR_SERVICE_NAMESPACE.to_string())
            .or_insert_with(|| "cardinalsin".to_string());
        if let Some(run_id) = &run_id {
            attr_map.insert(ATTR_CARDINALSIN_RUN_ID.to_string(), run_id.clone());
        }

        let attributes = attr_map
            .into_iter()
            .map(|(k, v)| KeyValue::new(k, v))
            .collect::<Vec<_>>();

        Ok(Self {
            mode,
            service_name: service_name.to_string(),
            otlp_endpoint: endpoint,
            otlp_protocol: protocol.to_string(),
            traces_sampler: sampler_name,
            run_id,
            resource_attributes: attributes,
            sampler,
        })
    }
}

/// Handle that keeps telemetry SDK providers alive for process lifetime.
pub struct Telemetry {
    config: TelemetryConfig,
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Telemetry {
    /// Initialize shared tracing + OTel SDK providers for a binary.
    pub fn init_for_component(default_service_name: &str, log_level: &str) -> Result<Self> {
        let config = TelemetryConfig::from_env(default_service_name)?;
        let level = parse_log_level(log_level)?;

        FmtSubscriber::builder()
            .with_max_level(level)
            .with_target(true)
            .with_thread_ids(true)
            .json()
            .try_init()
            .map_err(|e| {
                Error::Config(format!("failed to initialize telemetry subscriber: {e}"))
            })?;

        let resource =
            Resource::default().merge(&Resource::new(config.resource_attributes.clone()));

        let tracer_provider = TracerProvider::builder()
            .with_config(
                trace::Config::default()
                    .with_sampler(config.sampler.clone())
                    .with_resource(resource.clone()),
            )
            .build();
        let _ = global::set_tracer_provider(tracer_provider.clone());

        let meter_provider = SdkMeterProvider::builder().with_resource(resource).build();
        global::set_meter_provider(meter_provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());

        info!(
            service_name = %config.service_name,
            telemetry_mode = config.mode.as_str(),
            otlp_endpoint = %config.otlp_endpoint.as_deref().unwrap_or("none"),
            otlp_protocol = %config.otlp_protocol,
            traces_sampler = %config.traces_sampler,
            run_id = %config.run_id.as_deref().unwrap_or("none"),
            "Telemetry bootstrap initialized"
        );

        Ok(Self {
            config,
            tracer_provider,
            meter_provider,
        })
    }

    pub fn run_id(&self) -> Option<&str> {
        self.config.run_id.as_deref()
    }

    pub fn service_name(&self) -> &str {
        &self.config.service_name
    }

    pub fn mode(&self) -> &TelemetryMode {
        &self.config.mode
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        let _ = self.meter_provider.shutdown();
        let _ = self.tracer_provider.shutdown();
    }
}

fn parse_otlp_protocol(raw: &str) -> Result<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        OTEL_PROTOCOL_GRPC => Ok(OTEL_PROTOCOL_GRPC),
        OTEL_PROTOCOL_HTTP_PROTOBUF | "http/proto" | "http" => Ok(OTEL_PROTOCOL_HTTP_PROTOBUF),
        other => Err(Error::Config(format!(
            "OTEL_EXPORTER_OTLP_PROTOCOL must be one of [grpc, http/protobuf], got '{other}'"
        ))),
    }
}

fn parse_log_level(raw: &str) -> Result<Level> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(Level::TRACE),
        "debug" => Ok(Level::DEBUG),
        "info" => Ok(Level::INFO),
        "warn" => Ok(Level::WARN),
        "error" => Ok(Level::ERROR),
        other => Err(Error::Config(format!(
            "invalid log level '{other}', expected one of [trace, debug, info, warn, error]"
        ))),
    }
}

fn parse_optional_bool(name: &str) -> Result<Option<bool>> {
    let Some(raw) = std::env::var(name).ok() else {
        return Ok(None);
    };
    let value = raw.trim().to_ascii_lowercase();
    match value.as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(Error::Config(format!(
            "{name} must be a boolean (true/false/1/0), got '{raw}'"
        ))),
    }
}

fn parse_sampler(name: &str, arg: Option<&str>) -> Result<Sampler> {
    let lower = name.trim().to_ascii_lowercase();
    match lower.as_str() {
        "always_on" => Ok(Sampler::AlwaysOn),
        "always_off" => Ok(Sampler::AlwaysOff),
        "traceidratio" => Ok(Sampler::TraceIdRatioBased(parse_ratio(arg)?)),
        "parentbased_always_on" => Ok(Sampler::ParentBased(Box::new(Sampler::AlwaysOn))),
        "parentbased_always_off" => Ok(Sampler::ParentBased(Box::new(Sampler::AlwaysOff))),
        "parentbased_traceidratio" => Ok(Sampler::ParentBased(Box::new(
            Sampler::TraceIdRatioBased(parse_ratio(arg)?),
        ))),
        other => Err(Error::Config(format!(
            "OTEL_TRACES_SAMPLER '{other}' is not supported. Supported values: always_on, always_off, traceidratio, parentbased_always_on, parentbased_always_off, parentbased_traceidratio"
        ))),
    }
}

fn parse_ratio(arg: Option<&str>) -> Result<f64> {
    let raw = arg.ok_or_else(|| {
        Error::Config("OTEL_TRACES_SAMPLER_ARG is required for ratio samplers".to_string())
    })?;
    let value = raw.trim().parse::<f64>().map_err(|e| {
        Error::Config(format!(
            "OTEL_TRACES_SAMPLER_ARG must be a float in [0,1]: {e}"
        ))
    })?;
    if !(0.0..=1.0).contains(&value) {
        return Err(Error::Config(format!(
            "OTEL_TRACES_SAMPLER_ARG must be in [0,1], got {value}"
        )));
    }
    Ok(value)
}

fn parse_resource_attributes(raw: &str) -> Result<Vec<(String, String)>> {
    let mut attrs = Vec::new();
    for pair in raw.split(',') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(Error::Config(format!(
                "OTEL_RESOURCE_ATTRIBUTES entry '{trimmed}' is invalid, expected key=value"
            )));
        };

        let key = key.trim();
        if key.is_empty() {
            return Err(Error::Config(
                "OTEL_RESOURCE_ATTRIBUTES contains an empty attribute key".to_string(),
            ));
        }

        attrs.push((key.to_string(), value.trim().to_string()));
    }
    Ok(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_resource_attributes_accepts_valid_input() {
        let attrs = parse_resource_attributes("service.instance.id=abc,foo=bar").unwrap();
        assert_eq!(
            attrs,
            vec![
                ("service.instance.id".to_string(), "abc".to_string()),
                ("foo".to_string(), "bar".to_string())
            ]
        );
    }

    #[test]
    fn parse_resource_attributes_rejects_invalid_pairs() {
        let err = parse_resource_attributes("broken").unwrap_err();
        assert!(format!("{err}").contains("key=value"));
    }

    #[test]
    fn parse_sampler_requires_ratio_arg() {
        let err = parse_sampler("traceidratio", None).unwrap_err();
        assert!(format!("{err}").contains("OTEL_TRACES_SAMPLER_ARG"));
    }
}
