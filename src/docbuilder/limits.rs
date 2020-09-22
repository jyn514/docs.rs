use crate::db::Client;
use crate::error::Result;
use crate::Blocking;
use serde::Serialize;
use sqlx::query;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Limits {
    memory: usize,
    targets: usize,
    timeout: Duration,
    networking: bool,
    max_log_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory: 3 * 1024 * 1024 * 1024,        // 3 GB
            timeout: Duration::from_secs(15 * 60), // 15 minutes
            targets: 10,
            networking: false,
            max_log_size: 100 * 1024, // 100 KB
        }
    }
}

impl Limits {
    pub(crate) fn for_crate(conn: &mut Client, name: &str) -> Result<Self> {
        let mut limits = Self::default();

        let record = query!(
            "SELECT * FROM sandbox_overrides WHERE crate_name = $1;",
            name,
        )
        .fetch_optional(conn)
        .block()?;
        if let Some(rec) = record {
            if let Some(memory) = rec.max_memory_bytes {
                limits.memory = memory as usize;
            }
            if let Some(timeout) = rec.timeout_seconds {
                limits.timeout = Duration::from_secs(timeout as u64);
            }
            if let Some(targets) = rec.max_targets {
                limits.targets = targets as usize;
            } else if rec.timeout_seconds.is_some() {
                limits.targets = 1;
            }
        }

        Ok(limits)
    }

    pub(crate) fn memory(&self) -> usize {
        self.memory
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn networking(&self) -> bool {
        self.networking
    }

    pub(crate) fn max_log_size(&self) -> usize {
        self.max_log_size
    }

    pub(crate) fn targets(&self) -> usize {
        self.targets
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::*;
    use sqlx::Executor;

    #[test]
    fn retrieve_limits() {
        wrapper(|env| {
            let db = env.db();

            let krate = "hexponent";
            // limits work if no crate has limits set
            let hexponent = Limits::for_crate(&mut db.conn(), krate)?;
            assert_eq!(hexponent, Limits::default());

            db.conn()
                .execute(query!(
                    "INSERT INTO sandbox_overrides (crate_name, max_targets) VALUES ($1, 15)",
                    krate,
                ))
                .block()?;
            // limits work if crate has limits set
            let hexponent = Limits::for_crate(&mut db.conn(), krate)?;
            assert_eq!(
                hexponent,
                Limits {
                    targets: 15,
                    ..Limits::default()
                }
            );

            // all limits work
            let krate = "regex";
            let limits = Limits {
                memory: 100_000,
                timeout: Duration::from_secs(300),
                targets: 1,
                ..Limits::default()
            };
            db.conn().execute(query!(
                "INSERT INTO sandbox_overrides (crate_name, max_memory_bytes, timeout_seconds, max_targets)
                 VALUES ($1, $2, $3, $4)",
                krate, limits.memory as i64, limits.timeout.as_secs() as i32, limits.targets as i32,
            )).block()?;
            assert_eq!(limits, Limits::for_crate(&mut db.conn(), krate)?);
            Ok(())
        });
    }

    #[test]
    fn targets_default_to_one_with_timeout() {
        wrapper(|env| {
            let db = env.db();
            let krate = "hexponent";
            db.conn()
                .execute(query!(
                "INSERT INTO sandbox_overrides (crate_name, timeout_seconds) VALUES ($1, 20*60);",
                krate,
            ))
                .block()?;
            let limits = Limits::for_crate(&mut db.conn(), krate)?;
            assert_eq!(limits.targets, 1);

            Ok(())
        });
    }
}
