//! Assured S3 lineage and canonical state kernel.
//!
//! The implementation and its K1-K7 assurance apparatus are duplicated from
//! the V2 gateway donor without changing storage semantics. V3 owns the record
//! vocabulary it carries through this kernel.

#[allow(
    dead_code,
    missing_debug_implementations,
    clippy::must_use_candidate,
    clippy::result_unit_err
)]
pub mod state_kernel;

pub mod atomic_keyspace;

pub use atomic_keyspace::{
    AtomicKeyspace, DeleteBelowReport, DeleteOutcome, KEYSPACE_ROOT, KeyspaceError, TrimState,
};
pub use state_kernel::{KernelHandle, KernelInitError};
pub use terminal_read::{LineageHeadState, TerminalRecordRead};
pub use yeetz_sdk_s3::S3Config;

#[cfg(feature = "test-support")]
mod real_s3_aba_probe;
#[cfg(feature = "test-support")]
pub use real_s3_aba_probe::run_real_s3_aba_probe;

mod terminal_read;

#[cfg(test)]
mod public_boundary_contract {
    fn assert_no_public_adapter_type(source: &str) {
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);
        for tail in production.split("pub ").skip(1) {
            let declaration = tail.split('{').next().unwrap_or(tail);
            let exposes_item = [
                "fn ",
                "async fn ",
                "type ",
                "struct ",
                "enum ",
                "trait ",
                "static ",
                "const ",
            ]
            .iter()
            .any(|prefix| declaration.trim_start().starts_with(prefix));
            assert!(
                !exposes_item || !declaration.contains("ObjectStoreClient"),
                "public kernel declaration exposes ObjectStoreClient: {declaration}"
            );
        }
        for line in production.lines() {
            let line = line.trim_start();
            assert!(
                !line.starts_with("pub ") || !line.contains("ObjectStoreClient"),
                "public kernel field exposes ObjectStoreClient: {line}"
            );
        }
    }

    #[test]
    fn adapter_type_is_absent_from_the_public_kernel_surface() {
        assert_no_public_adapter_type(include_str!("state_kernel.rs"));
        let atomic_keyspace = include_str!("atomic_keyspace.rs");
        assert_no_public_adapter_type(atomic_keyspace);
        assert!(
            !atomic_keyspace.contains("pub async fn get_with_version("),
            "the internal value-envelope version escaped into the production API"
        );
        assert_no_public_adapter_type(include_str!("terminal_read.rs"));
    }
}
