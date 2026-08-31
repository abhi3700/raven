//! Durable progress tracking for `raven run`.
//!
//! A checkpoint is not merely the latest block observed by the event source. It is the latest
//! canonical state that Raven has delivered successfully to **every** plugin. The runner therefore
//! updates this file only after all plugin acknowledgements for an event have succeeded. On
//! restart, `--start resume` uses this state to continue after the last fully acknowledged event;
//! an event that was only partly processed before a crash is replayed instead of being silently
//! skipped.
//!
//! ```text
//! source event
//!      |
//!      v
//! all plugins acknowledge success
//!      |
//!      v
//! update bounded canonical window
//!      |
//!      v
//! write temp file -> fsync file -> atomic rename -> fsync directory
//!      |
//!      v
//! durable resume point
//! ```
//!
//! The checkpoint retains a bounded, oldest-to-newest window of block payloads rather than only a
//! block number. Block hashes and parent hashes let Raven verify ancestry and represent a shallow
//! reorganization as stack operations; retained logs let plugins receive the same data on apply
//! and revert. An applied block must extend the current tip, while a reverted block must exactly
//! match and remove the current tip. Keeping one file per chain and validating its chain ID also
//! prevents progress from one EVM chain being reused on another.
//!
//! Persistence uses a temporary file followed by an atomic rename. A crash before the rename leaves
//! the previous checkpoint authoritative, which may replay work but must not skip it. Consequently,
//! checkpointing provides at-least-once recovery, not exactly-once side effects inside plugins.

use crate::config;
use eyre::{Result, WrapErr, bail};
use raven_core::{BlockEvent, ChainEvent, ChainId};
use serde::{Deserialize, Serialize};
use std::{
	fs::{self, OpenOptions},
	io::{self, Write},
	path::{Path, PathBuf},
};

/// Version of the checkpoint's on-disk JSON contract.
///
/// Checkpoints can outlive the Raven binary that wrote them. This independent version marker lets a
/// newer binary distinguish a compatible checkpoint from data whose layout or meaning has changed.
/// Without it, an old file might fail with a vague Serde error or, worse, deserialize while being
/// interpreted with new semantics. Raven currently accepts only an exact match and fails closed.
///
/// Increment this value only for an incompatible persisted-format or semantic change, and add an
/// explicit migration/compatibility path before accepting older versions. It is not the Raven
/// release version, the configured chain ID, or an EVM protocol version.
const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

/// CLI-owned durable acknowledgement and bounded canonical ancestry.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Checkpoint {
	/// Selects the decoder and semantics for this persisted record.
	schema_version: u32,
	/// Identifies the EVM chain whose progress this record describes.
	chain_id: ChainId,
	/// Successfully acknowledged canonical blocks, ordered oldest to newest.
	blocks: Vec<BlockEvent>,
}

impl Checkpoint {
	/// Creates an in-memory starting point with no durable progress yet.
	pub(crate) fn empty(chain_id: ChainId) -> Self {
		Self { schema_version: CHECKPOINT_SCHEMA_VERSION, chain_id, blocks: Vec::new() }
	}

	/// Loads and validates the checkpoint for `chain_id`, if one has been persisted.
	///
	/// Schema, chain, and canonical-window validation happens before the value reaches the runner
	/// so corrupt or incompatible recovery state cannot silently influence source startup.
	pub(crate) fn load(chain_id: ChainId) -> Result<Option<Self>> {
		load_from(&checkpoint_path(chain_id)?)
			.map(|checkpoint| {
				checkpoint.map(|checkpoint| checkpoint.validate(chain_id)).transpose()
			})
			.and_then(|result| result)
	}

	/// Returns the retained canonical ancestry used to initialize a resumable source.
	pub(crate) fn blocks(&self) -> &[BlockEvent] {
		&self.blocks
	}

	/// Applies one fully acknowledged event and persists the resulting resume point.
	///
	/// The runner must call this only after every plugin has acknowledged the event. Any returned
	/// I/O error is terminal for that run: the previously installed checkpoint remains the
	/// authoritative recovery point.
	pub(crate) fn apply_and_save(&mut self, event: &ChainEvent, reorg_depth: usize) -> Result<()> {
		self.apply(event, reorg_depth)?;
		save_to(&checkpoint_path(self.chain_id)?, self)
	}

	fn validate(self, expected_chain_id: ChainId) -> Result<Self> {
		if self.schema_version != CHECKPOINT_SCHEMA_VERSION {
			bail!(
				"unsupported checkpoint schema version {}; expected {}",
				self.schema_version,
				CHECKPOINT_SCHEMA_VERSION
			);
		}
		if self.chain_id != expected_chain_id {
			bail!(
				"checkpoint chain {} does not match RPC chain {}",
				self.chain_id.get(),
				expected_chain_id.get()
			);
		}
		validate_window(&self.blocks, self.chain_id)?;
		Ok(self)
	}

	/// Applies the event as a push or pop on the bounded canonical-chain window.
	fn apply(&mut self, event: &ChainEvent, reorg_depth: usize) -> Result<()> {
		if event.chain_id() != self.chain_id {
			bail!(
				"cannot checkpoint chain {} event in chain {} checkpoint",
				event.chain_id().get(),
				self.chain_id.get()
			);
		}

		match event {
			ChainEvent::BlockApplied(block) => {
				if let Some(tip) = self.blocks.last() {
					if tip.block_number() == block.block_number() &&
						tip.block_hash() == block.block_hash()
					{
						return Ok(());
					}
					if block.block_number() != tip.block_number().saturating_add(1) ||
						block.parent_hash() != tip.block_hash()
					{
						bail!(
							"applied block {} does not extend checkpoint tip {}",
							block.block_number(),
							tip.block_number()
						);
					}
				}
				self.blocks.push(block.clone());
				if self.blocks.len() > reorg_depth {
					self.blocks.drain(..self.blocks.len() - reorg_depth);
				}
			},
			ChainEvent::BlockReverted(block) => {
				let Some(tip) = self.blocks.last() else {
					bail!("cannot revert block {} from an empty checkpoint", block.block_number());
				};
				if tip.block_number() != block.block_number() ||
					tip.block_hash() != block.block_hash()
				{
					bail!(
						"reverted block {} ({}) does not match checkpoint tip {} ({})",
						block.block_number(),
						block.block_hash(),
						tip.block_number(),
						tip.block_hash()
					);
				}
				self.blocks.pop();
			},
		}
		Ok(())
	}
}

fn validate_window(blocks: &[BlockEvent], chain_id: ChainId) -> Result<()> {
	if blocks.iter().any(|block| block.chain_id() != chain_id) {
		bail!("checkpoint contains a block from another chain");
	}
	for pair in blocks.windows(2) {
		if pair[1].block_number() != pair[0].block_number().saturating_add(1) ||
			pair[1].parent_hash() != pair[0].block_hash()
		{
			bail!("checkpoint canonical window is not contiguous");
		}
	}
	Ok(())
}

fn checkpoint_path(chain_id: ChainId) -> Result<PathBuf> {
	Ok(config::config_directory()?
		.join("checkpoints")
		.join(format!("{}.json", chain_id.get())))
}

fn load_from(path: &Path) -> Result<Option<Checkpoint>> {
	let contents = match fs::read_to_string(path) {
		Ok(contents) => contents,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => {
			return Err(error)
				.wrap_err_with(|| format!("failed to read checkpoint {}", path.display()));
		},
	};
	serde_json::from_str(&contents)
		.map(Some)
		.wrap_err_with(|| format!("failed to parse checkpoint {}", path.display()))
}

/// Durably replaces `path` without exposing a partially written JSON document.
///
/// Syncing the file persists its contents; the atomic rename installs it; syncing the containing
/// directory persists that rename on Unix. If installation fails, the temporary file is removed on
/// a best-effort basis and the previous checkpoint remains usable.
fn save_to(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
	let parent = path.parent().ok_or_else(|| eyre::eyre!("checkpoint path has no parent"))?;
	fs::create_dir_all(parent)
		.wrap_err_with(|| format!("failed to create checkpoint directory {}", parent.display()))?;
	let mut contents =
		serde_json::to_vec_pretty(checkpoint).wrap_err("failed to serialize checkpoint")?;
	contents.push(b'\n');
	let temporary_path = path.with_extension(format!("json.tmp-{}", std::process::id()));

	let write_result = (|| -> Result<()> {
		let mut options = OpenOptions::new();
		options.create(true).truncate(true).write(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(0o600);
		}
		let mut file = options
			.open(&temporary_path)
			.wrap_err("failed to create checkpoint temp file")?;
		file.write_all(&contents).wrap_err("failed to write checkpoint")?;
		file.sync_all().wrap_err("failed to sync checkpoint")?;
		#[cfg(target_os = "windows")]
		if path.exists() {
			fs::remove_file(path).wrap_err("failed to replace checkpoint")?;
		}
		fs::rename(&temporary_path, path).wrap_err("failed to install checkpoint")?;
		#[cfg(unix)]
		fs::File::open(parent)?
			.sync_all()
			.wrap_err("failed to sync checkpoint directory")?;
		Ok(())
	})();

	if write_result.is_err() {
		let _ = fs::remove_file(&temporary_path);
	}
	write_result
}

#[cfg(test)]
mod tests {
	use super::*;
	use alloy_primitives::{Address, B256, Bytes};
	use raven_core::EvmLog;
	use std::sync::atomic::{AtomicU64, Ordering};

	static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(1);

	fn block(number: u64, hash: u64, parent: u64) -> BlockEvent {
		BlockEvent::new(
			ChainId::new(8453).unwrap(),
			number,
			format!("0x{hash:064x}").parse::<B256>().unwrap(),
			format!("0x{parent:064x}").parse::<B256>().unwrap(),
			1,
			0,
		)
		.unwrap()
	}

	fn block_with_log(number: u64, hash: u64, parent: u64) -> BlockEvent {
		let log = EvmLog::new(
			Address::repeat_byte(0x11),
			vec![B256::repeat_byte(0x22)],
			Bytes::from_static(&[0x33]),
			B256::repeat_byte(0x44),
			0,
			0,
		)
		.unwrap();
		BlockEvent::new_with_logs(
			ChainId::new(8453).unwrap(),
			number,
			format!("0x{hash:064x}").parse::<B256>().unwrap(),
			format!("0x{parent:064x}").parse::<B256>().unwrap(),
			1,
			1,
			vec![log],
		)
		.unwrap()
	}

	/// apply #10        apply #11        revert #11
	///    │                 │                 │
	///    ▼                 ▼                 ▼
	///   []   ───────▶    [10]   ───────▶  [10, 11]   ───────▶  [10]
	///                                      ▲
	///                                      └── current tip
	#[test]
	fn advances_and_rewinds_only_exact_canonical_tip() {
		let mut checkpoint = Checkpoint::empty(ChainId::new(8453).unwrap());
		let a = block(10, 10, 9);
		let b = block(11, 11, 10);
		checkpoint.apply(&ChainEvent::BlockApplied(a), 64).unwrap();
		checkpoint.apply(&ChainEvent::BlockApplied(b.clone()), 64).unwrap();
		checkpoint.apply(&ChainEvent::BlockReverted(b), 64).unwrap();
		assert_eq!(checkpoint.blocks.len(), 1);
		assert_eq!(checkpoint.blocks[0].block_number(), 10);
	}

	/// apply #10                    try apply #12
	///    │                              │
	///    ▼                              ▼
	///   []   ──────────────▶          [10]
	///                                  │
	///                                  ├── expected: #11
	///                                  │
	///                                  └── received: #12  ✗
	///
	/// Would produce:
	///
	///        #10  ──X──>  #12
	///               │
	///            #11 missing
	///
	/// Therefore `[10, 12]` is rejected because the canonical chain must be contiguous.
	#[test]
	fn rejects_non_contiguous_applied_block() {
		let mut checkpoint = Checkpoint::empty(ChainId::new(8453).unwrap());
		checkpoint.apply(&ChainEvent::BlockApplied(block(10, 10, 9)), 64).unwrap();
		assert!(checkpoint.apply(&ChainEvent::BlockApplied(block(12, 12, 11)), 64).is_err());
	}

	/// Verifies that a checkpoint survives a save → load round trip.
	///
	/// ```text
	/// In memory
	///
	///   [] ──apply #10──▶ [10] ──apply #11──▶ [10, 11]
	///                                            │
	///                                            │ save_to(path)
	///                                            ▼
	///                                   ┌─────────────────┐
	///                                   │ checkpoint.json │
	///                                   │    [10, 11]     │
	///                                   └────────┬────────┘
	///                                            │
	///                                            │ load_from(path)
	///                                            ▼
	///                                       [10, 11]
	///                                            ▲
	///                                            │
	///                                  restored tip = #11 ✓
	/// ```
	///
	/// This ensures Raven can restart from the last successfully persisted canonical block.
	#[test]
	fn durable_checkpoint_round_trip_resumes_after_last_success() {
		let path = std::env::temp_dir().join(format!(
			"raven-checkpoint-test-{}-{}.json",
			std::process::id(),
			NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed)
		));
		let mut checkpoint = Checkpoint::empty(ChainId::new(8453).unwrap());
		checkpoint.apply(&ChainEvent::BlockApplied(block(10, 10, 9)), 64).unwrap();
		checkpoint
			.apply(&ChainEvent::BlockApplied(block_with_log(11, 11, 10)), 64)
			.unwrap();
		save_to(&path, &checkpoint).unwrap();

		let restored = load_from(&path)
			.unwrap()
			.unwrap()
			.validate(ChainId::new(8453).unwrap())
			.unwrap();
		assert_eq!(restored.blocks.last().unwrap().block_number(), 11);
		assert_eq!(restored.blocks.last().unwrap().block_hash(), block(11, 11, 10).block_hash());
		assert_eq!(restored.blocks.last().unwrap().logs().len(), 1);
		let _ = fs::remove_file(path);
	}
}
