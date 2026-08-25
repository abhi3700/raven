use std::{borrow::Cow, fmt};

use tracing::field::{Field, Visit};
use tracing_subscriber::{
	field::RecordFields,
	fmt::format::{FormatFields, Writer},
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

#[derive(Debug, Default)]
struct RavenFields;

pub(crate) fn init() {
	tracing_subscriber::fmt()
		.fmt_fields(RavenFields)
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| "raven=info,raven_source_alloy=info".into()),
		)
		.init();
}

impl<'writer> FormatFields<'writer> for RavenFields {
	fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
		let mut visitor = RavenFieldVisitor { writer, is_empty: true, result: Ok(()) };
		fields.record(&mut visitor);
		visitor.result
	}
}

struct RavenFieldVisitor<'writer> {
	writer: Writer<'writer>,
	is_empty: bool,
	result: fmt::Result,
}

impl RavenFieldVisitor<'_> {
	fn write_message(&mut self, value: &str) {
		if !self.pad() {
			return;
		}

		let value = sanitize(value);
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

	fn capture_log(ansi: bool, emit: impl FnOnce()) -> String {
		let buffer = SharedBuffer::default();
		let subscriber = tracing_subscriber::fmt()
			.with_ansi(ansi)
			.without_time()
			.with_target(false)
			.fmt_fields(RavenFields)
			.with_writer(buffer.clone())
			.finish();

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
