use std::{borrow::Cow, fmt};

use tracing::{
	Event, Subscriber,
	field::{Field, Visit},
	span::{Attributes, Id},
};
use tracing_subscriber::{
	field::RecordFields,
	fmt::{
		FmtContext,
		format::{FormatEvent, FormatFields, Writer},
		time::{FormatTime, SystemTime},
	},
	layer::{Context, Layer},
	prelude::*,
	registry::LookupSpan,
};

const RESET: &str = "\x1b[0m";
const FIELD_KEY: &str = "\x1b[3;90m";
const FIELD_EQUALS: &str = "\x1b[2m";
const STRING_VALUE: &str = "\x1b[96m";
const NUMBER_VALUE: &str = "\x1b[93m";
const TRUE_VALUE: &str = "\x1b[92m";
const FALSE_VALUE: &str = "\x1b[91m";
const HASH_VALUE: &str = "\x1b[95m";
const ERROR_VALUE: &str = "\x1b[91m";

const PLUGIN_SPAN_NAME: &str = "raven.plugin";
const DEFAULT_LOG_FILTER: &str = "raven=info,raven_plugin_=info,raven_source_rpc=info";

/// Terminal color assigned by the CLI when a plugin is registered.
///
/// Consecutive registrations walk the color wheel using a coprime stride, so the
/// first 360 plugins receive distinct hues while the assignment remains stable
/// for the same registration order across process restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PluginColor {
	red: u8,
	green: u8,
	blue: u8,
}

impl PluginColor {
	fn from_registration(index: u32) -> Self {
		const HUE_OFFSET: u32 = 210;
		const HUE_STRIDE: u32 = 137;

		let hue = (HUE_OFFSET + index.wrapping_mul(HUE_STRIDE)) % 360;
		let sector = hue / 60;
		let fraction = hue % 60;
		let rising = (fraction * 255 / 60) as u8;
		let falling = 255 - rising;
		let (red, green, blue) = match sector {
			0 => (255, rising, 0),
			1 => (falling, 255, 0),
			2 => (0, 255, rising),
			3 => (0, falling, 255),
			4 => (rising, 0, 255),
			_ => (255, 0, falling),
		};

		Self { red, green, blue }
	}

	const fn encoded(self) -> u64 {
		((self.red as u64) << 16) | ((self.green as u64) << 8) | self.blue as u64
	}

	const fn from_encoded(encoded: u64) -> Option<Self> {
		if encoded > 0x00ff_ffff {
			return None;
		}

		Some(Self { red: (encoded >> 16) as u8, green: (encoded >> 8) as u8, blue: encoded as u8 })
	}
}

/// Allocates stable, distinct terminal colors as plugins are registered.
#[derive(Debug, Default)]
pub(crate) struct PluginColorAllocator {
	next: u32,
}

impl PluginColorAllocator {
	pub(crate) fn assign(&mut self) -> PluginColor {
		let color = PluginColor::from_registration(self.next);
		self.next = self.next.wrapping_add(1);
		color
	}
}

/// Plugin identity carried by a tracing span around each plugin lifecycle call.
#[derive(Debug, Clone)]
pub(crate) struct PluginLogIdentity {
	name: String,
	color: PluginColor,
}

impl PluginLogIdentity {
	pub(crate) fn new(name: impl Into<String>, color: PluginColor) -> Self {
		Self { name: name.into(), color }
	}
}

/// Creates the tracing context used by the CLI's plugin wrapper.
///
/// The error span level keeps this context enabled whenever a plugin event can
/// be emitted by the configured `RUST_LOG` filter, including warnings when
/// informational logs are suppressed.
pub(crate) fn plugin_span(identity: &PluginLogIdentity) -> tracing::Span {
	tracing::span!(
		target: "raven::plugin",
		tracing::Level::ERROR,
		PLUGIN_SPAN_NAME,
		plugin_name = identity.name.as_str(),
		plugin_color = identity.color.encoded(),
	)
}

#[derive(Debug, Default)]
struct RavenFields;

pub(crate) fn init() {
	let filter = tracing_subscriber::EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| DEFAULT_LOG_FILTER.into());

	tracing_subscriber::registry()
		.with(PluginSpanLayer)
		.with(
			tracing_subscriber::fmt::layer()
				.fmt_fields(RavenFields)
				.event_format(RavenEventFormatter)
				.with_filter(filter),
		)
		.init();
}

/// Stores plugin identity in tracing span extensions for terminal formatting.
#[derive(Debug, Default)]
struct PluginSpanLayer;

impl<S> Layer<S> for PluginSpanLayer
where
	S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
	fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
		if attributes.metadata().name() != PLUGIN_SPAN_NAME {
			return;
		}

		let mut visitor = PluginSpanVisitor::default();
		attributes.record(&mut visitor);
		let Some(identity) = visitor.into_identity() else { return };
		let Some(span) = context.span(id) else { return };

		span.extensions_mut().insert(identity);
	}
}

#[derive(Debug, Default)]
struct PluginSpanVisitor {
	name: Option<String>,
	color: Option<PluginColor>,
}

impl PluginSpanVisitor {
	fn into_identity(self) -> Option<PluginLogIdentity> {
		Some(PluginLogIdentity::new(self.name?, self.color?))
	}
}

impl Visit for PluginSpanVisitor {
	fn record_str(&mut self, field: &Field, value: &str) {
		if field.name() == "plugin_name" {
			self.name = Some(value.to_owned());
		}
	}

	fn record_u64(&mut self, field: &Field, value: u64) {
		if field.name() == "plugin_color" {
			self.color = PluginColor::from_encoded(value);
		}
	}

	fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// Formats a compact, colored plugin tag before plugin-originated log events.
///
/// The default `tracing-subscriber` full formatter renders span fields inline,
/// which would repeat implementation-only span metadata on every event. This
/// formatter reads only Raven's plugin span extension and leaves all other
/// source/runtime events in the existing timestamp, level, target, and field
/// layout.
#[derive(Debug, Default)]
struct RavenEventFormatter;

impl<S, N> FormatEvent<S, N> for RavenEventFormatter
where
	S: Subscriber + for<'lookup> LookupSpan<'lookup>,
	N: for<'writer> FormatFields<'writer> + 'static,
{
	fn format_event(
		&self,
		context: &FmtContext<'_, S, N>,
		mut writer: Writer<'_>,
		event: &Event<'_>,
	) -> fmt::Result {
		SystemTime.format_time(&mut writer)?;
		writer.write_char(' ')?;
		write_level(&mut writer, *event.metadata().level())?;
		writer.write_char(' ')?;

		if let Some(identity) = plugin_identity(context) {
			write_plugin_tag(&mut writer, &identity)?;
			writer.write_char(' ')?;
		}

		let target = event.metadata().target();
		if writer.has_ansi_escapes() {
			write!(writer, "{FIELD_KEY}{target}{RESET}{FIELD_EQUALS}:{RESET} ")?;
		} else {
			write!(writer, "{target}: ")?;
		}
		context.format_fields(writer.by_ref(), event)?;
		writeln!(writer)
	}
}

fn plugin_identity<S, N>(context: &FmtContext<'_, S, N>) -> Option<PluginLogIdentity>
where
	S: Subscriber + for<'lookup> LookupSpan<'lookup>,
	N: for<'writer> FormatFields<'writer> + 'static,
{
	context.event_scope().and_then(|scope| {
		scope
			.from_root()
			.find_map(|span| span.extensions().get::<PluginLogIdentity>().cloned())
	})
}

fn write_level(writer: &mut Writer<'_>, level: tracing::Level) -> fmt::Result {
	let level = format!("{level:>5}");
	if !writer.has_ansi_escapes() {
		return writer.write_str(&level);
	}

	let color = match level.trim() {
		"ERROR" => "\x1b[31m",
		"WARN" => "\x1b[33m",
		"INFO" => "\x1b[32m",
		"DEBUG" => "\x1b[34m",
		_ => "\x1b[35m",
	};
	write!(writer, "{color}{level}{RESET}")
}

fn write_plugin_tag(writer: &mut Writer<'_>, identity: &PluginLogIdentity) -> fmt::Result {
	if !writer.has_ansi_escapes() {
		return write!(writer, "[ {} ]", identity.name);
	}

	write!(
		writer,
		"\x1b[1;38;2;{};{};{}m[ {} ]{RESET}",
		identity.color.red, identity.color.green, identity.color.blue, identity.name
	)
}

impl<'writer> FormatFields<'writer> for RavenFields {
	fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
		let mut marker = TerminalReportMarker::default();
		fields.record(&mut marker);

		let mut visitor = RavenFieldVisitor {
			writer,
			is_empty: true,
			allows_multiline_message: marker.enabled,
			result: Ok(()),
		};
		fields.record(&mut visitor);
		visitor.result
	}
}

#[derive(Default)]
struct TerminalReportMarker {
	enabled: bool,
}

impl Visit for TerminalReportMarker {
	fn record_bool(&mut self, field: &Field, value: bool) {
		if field.name() == "raven_terminal_report" {
			self.enabled = value;
		}
	}

	fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

struct RavenFieldVisitor<'writer> {
	writer: Writer<'writer>,
	is_empty: bool,
	allows_multiline_message: bool,
	result: fmt::Result,
}

impl RavenFieldVisitor<'_> {
	fn write_message(&mut self, value: &str) {
		if !self.pad() {
			return;
		}

		let value = if self.allows_multiline_message {
			sanitize_terminal_report(value)
		} else {
			sanitize(value)
		};
		self.result = write!(self.writer, "{value}");
	}

	fn write_field(&mut self, field: &Field, value: &str, color: &str) {
		if !self.pad() {
			return;
		}

		let name = field.name().strip_prefix("r#").unwrap_or(field.name());
		let ansi = self.writer.has_ansi_escapes();
		let value = sanitize(value);

		self.result = if ansi {
			write!(
				self.writer,
				"{FIELD_KEY}{name}{RESET}{FIELD_EQUALS}={RESET}{color}{value}{RESET}"
			)
		} else {
			write!(self.writer, "{name}={value}")
		};
	}

	fn write_debug_field(&mut self, field: &Field, value: &dyn fmt::Debug) {
		let rendered = format!("{value:?}");
		if field.name() == "message" {
			self.write_message(&rendered);
			return;
		}

		let color = if is_error_field(field.name()) {
			ERROR_VALUE
		} else if field.name().contains("hash") {
			HASH_VALUE
		} else {
			STRING_VALUE
		};
		self.write_field(field, &rendered, color);
	}

	fn pad(&mut self) -> bool {
		if self.result.is_err() {
			return false;
		}

		if self.is_empty {
			self.is_empty = false;
			return true;
		}

		self.result = self.writer.write_char(' ');
		self.result.is_ok()
	}
}

impl Visit for RavenFieldVisitor<'_> {
	fn record_f64(&mut self, field: &Field, value: f64) {
		self.write_field(field, &value.to_string(), NUMBER_VALUE);
	}

	fn record_i64(&mut self, field: &Field, value: i64) {
		self.write_field(field, &value.to_string(), NUMBER_VALUE);
	}

	fn record_u64(&mut self, field: &Field, value: u64) {
		self.write_field(field, &value.to_string(), NUMBER_VALUE);
	}

	fn record_i128(&mut self, field: &Field, value: i128) {
		self.write_field(field, &value.to_string(), NUMBER_VALUE);
	}

	fn record_u128(&mut self, field: &Field, value: u128) {
		self.write_field(field, &value.to_string(), NUMBER_VALUE);
	}

	fn record_bool(&mut self, field: &Field, value: bool) {
		if field.name() == "raven_terminal_report" {
			self.allows_multiline_message = value;
			return;
		}

		let color = if value { TRUE_VALUE } else { FALSE_VALUE };
		self.write_field(field, &value.to_string(), color);
	}

	fn record_str(&mut self, field: &Field, value: &str) {
		if field.name() == "message" {
			self.write_message(value);
		} else {
			self.write_field(field, &format!("{value:?}"), STRING_VALUE);
		}
	}

	fn record_bytes(&mut self, field: &Field, value: &[u8]) {
		self.write_field(field, &format!("{value:02x?}"), HASH_VALUE);
	}

	fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
		self.write_field(field, &value.to_string(), ERROR_VALUE);
	}

	fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
		self.write_debug_field(field, value);
	}
}

fn is_error_field(name: &str) -> bool {
	matches!(name, "error" | "panic") || name.ends_with("_error")
}

fn sanitize(value: &str) -> Cow<'_, str> {
	if !value
		.chars()
		.any(|character| matches!(character, '\x1b' | '\u{009b}' | '\n' | '\r'))
	{
		return Cow::Borrowed(value);
	}

	let mut sanitized = String::with_capacity(value.len());
	for character in value.chars() {
		match character {
			'\x1b' => sanitized.push_str("\\x1b"),
			'\u{009b}' => sanitized.push_str("\\u009b"),
			'\n' => sanitized.push_str("\\n"),
			'\r' => sanitized.push_str("\\r"),
			_ => sanitized.push(character),
		}
	}

	Cow::Owned(sanitized)
}

/// Preserves deliberate line breaks in terminal reports while still preventing
/// escape sequences or carriage returns from controlling the terminal.
fn sanitize_terminal_report(value: &str) -> Cow<'_, str> {
	if !value.chars().any(|character| matches!(character, '\x1b' | '\u{009b}' | '\r')) {
		return Cow::Borrowed(value);
	}

	let mut sanitized = String::with_capacity(value.len());
	for character in value.chars() {
		match character {
			'\x1b' => sanitized.push_str("\\x1b"),
			'\u{009b}' => sanitized.push_str("\\u009b"),
			'\r' => sanitized.push_str("\\r"),
			_ => sanitized.push(character),
		}
	}

	Cow::Owned(sanitized)
}

#[cfg(test)]
mod tests {
	use std::{
		io,
		sync::{Arc, Mutex},
	};

	use tracing_subscriber::fmt::MakeWriter;

	use super::*;

	#[test]
	fn colors_structured_values_by_type() {
		let output = capture_log(true, || {
			tracing::info!(
				count = 42_u64,
				connected = true,
				transport = "rpc",
				block_hash = %"0xabc",
				"connected"
			);
		});

		assert!(output.contains("\x1b[93m42\x1b[0m"));
		assert!(output.contains("\x1b[92mtrue\x1b[0m"));
		assert!(output.contains("\x1b[96m\"rpc\"\x1b[0m"));
		assert!(output.contains("\x1b[95m0xabc\x1b[0m"));
	}

	#[test]
	fn emits_plain_fields_when_ansi_is_disabled() {
		let output = capture_log(false, || {
			tracing::info!(count = 42_u64, connected = true, "connected");
		});

		assert!(!output.contains("\x1b["));
		assert!(output.contains("count=42"));
		assert!(output.contains("connected=true"));
	}

	#[test]
	fn neutralizes_ansi_sequences_inside_values() {
		let output = capture_log(true, || {
			tracing::info!(rpc_url = %"\x1b[31mevil", "connecting");
		});

		assert!(output.contains("\\x1b[31mevil"));
	}

	#[test]
	fn preserves_marked_terminal_report_line_breaks() {
		let output = capture_log(false, || {
			tracing::info!(raven_terminal_report = true, "+-- report\n| transfer\n+--");
		});

		assert!(output.contains("+-- report\n| transfer\n+--"));
		assert!(!output.contains("raven_terminal_report"));
	}

	#[test]
	fn terminal_reports_still_neutralize_ansi_sequences() {
		let output = capture_log(false, || {
			tracing::info!(raven_terminal_report = true, "+-- \x1b[31mreport\n+--");
		});

		assert!(output.contains("+-- \\x1b[31mreport\n+--"));
	}

	#[test]
	fn prefixes_plugin_events_with_a_color_coded_name_tag() {
		let output = capture_log(true, || {
			let mut colors = PluginColorAllocator::default();
			let identity = PluginLogIdentity::new("erc20-transfer", colors.assign());
			let span = plugin_span(&identity);
			let _guard = span.enter();
			tracing::info!("large transfer detected");
		});

		assert!(output.contains("\x1b[1;38;2;"));
		assert!(output.contains("[ erc20-transfer ]"));
	}

	#[test]
	fn preserves_plugin_name_tags_without_ansi() {
		let output = capture_log(false, || {
			let mut colors = PluginColorAllocator::default();
			let identity = PluginLogIdentity::new("reorg-monitor", colors.assign());
			let span = plugin_span(&identity);
			let _guard = span.enter();
			tracing::warn!("block reverted");
		});

		assert!(!output.contains("\x1b["));
		assert!(output.contains("[ reorg-monitor ]"));
	}

	#[test]
	fn allocates_distinct_hues_for_the_first_360_plugins() {
		let mut colors = PluginColorAllocator::default();
		let allocated: std::collections::HashSet<_> = (0..360).map(|_| colors.assign()).collect();

		assert_eq!(allocated.len(), 360);
	}

	#[test]
	fn default_filter_emits_packaged_plugin_events() {
		let output = capture_default_filtered_log(false, || {
			let mut colors = PluginColorAllocator::default();
			let identity = PluginLogIdentity::new("erc20-transfer", colors.assign());
			let span = plugin_span(&identity);
			let _guard = span.enter();
			tracing::info!(target: "raven_plugin_erc20_transfer", "large transfer detected");
		});

		assert!(output.contains("[ erc20-transfer ]"));
		assert!(output.contains("large transfer detected"));
	}

	fn capture_log(ansi: bool, emit: impl FnOnce()) -> String {
		let buffer = SharedBuffer::default();
		let subscriber = tracing_subscriber::registry().with(PluginSpanLayer).with(
			tracing_subscriber::fmt::layer()
				.with_ansi(ansi)
				.fmt_fields(RavenFields)
				.event_format(RavenEventFormatter)
				.with_writer(buffer.clone()),
		);

		tracing::subscriber::with_default(subscriber, emit);
		buffer.contents()
	}

	fn capture_default_filtered_log(ansi: bool, emit: impl FnOnce()) -> String {
		let buffer = SharedBuffer::default();
		let filter = tracing_subscriber::EnvFilter::try_new(DEFAULT_LOG_FILTER).unwrap();
		let subscriber = tracing_subscriber::registry().with(PluginSpanLayer).with(
			tracing_subscriber::fmt::layer()
				.with_ansi(ansi)
				.fmt_fields(RavenFields)
				.event_format(RavenEventFormatter)
				.with_writer(buffer.clone())
				.with_filter(filter),
		);

		tracing::subscriber::with_default(subscriber, emit);
		buffer.contents()
	}

	#[derive(Clone, Default)]
	struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

	impl SharedBuffer {
		fn contents(&self) -> String {
			let bytes = self.0.lock().expect("log buffer lock should not be poisoned").clone();
			String::from_utf8(bytes).expect("formatted logs should be valid UTF-8")
		}
	}

	struct SharedWriter(Arc<Mutex<Vec<u8>>>);

	impl io::Write for SharedWriter {
		fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
			self.0
				.lock()
				.expect("log buffer lock should not be poisoned")
				.extend_from_slice(bytes);
			Ok(bytes.len())
		}

		fn flush(&mut self) -> io::Result<()> {
			Ok(())
		}
	}

	impl<'writer> MakeWriter<'writer> for SharedBuffer {
		type Writer = SharedWriter;

		fn make_writer(&'writer self) -> Self::Writer {
			SharedWriter(Arc::clone(&self.0))
		}
	}
}
