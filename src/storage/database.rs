use super::{Blob, StorageTransaction};
use crate::db::Pool;
use crate::{Blocking, Metrics};
use chrono::{DateTime, Utc};
use failure::Error;
use sqlx::{query, Connection, Transaction};
use std::sync::Arc;

pub(crate) struct DatabaseBackend {
    pool: Pool,
    metrics: Arc<Metrics>,
}

impl DatabaseBackend {
    pub(crate) fn new(pool: Pool, metrics: Arc<Metrics>) -> Self {
        Self { pool, metrics }
    }

    pub(super) fn exists(&self, path: &str) -> Result<bool, Error> {
        // as exists! is https://github.com/launchbadge/sqlx/issues/696
        Ok(query!(
            r#"SELECT COUNT(*) > 0 as "exists!" FROM files WHERE path = $1"#,
            path
        )
        .fetch_one(&mut self.pool.get()?)
        .block()?
        .exists)
    }

    pub(super) fn get(&self, path: &str, max_size: usize) -> Result<Blob, Error> {
        use std::convert::TryInto;
        use std::io;

        // The maximum size for a BYTEA (the type used for `content`) is 1GB, so this cast is safe:
        // https://www.postgresql.org/message-id/162867790712200946i7ba8eb92v908ac595c0c35aee%40mail.gmail.com
        let max_size = max_size.min(std::i32::MAX as usize) as i32;

        // The size limit is checked at the database level, to avoid receiving data altogether if
        // the limit is exceeded.
        let record = query!(
            r#"SELECT
                 path, mime, date_updated, compression,
                 (CASE WHEN LENGTH(content) <= $2 THEN content ELSE NULL END) AS content,
                 (LENGTH(content) > $2) AS "is_too_big!"
             FROM files
             WHERE path = $1;"#,
            path,
            max_size,
        )
        .fetch_optional(&mut self.pool.get()?)
        .block()?
        .ok_or(super::PathNotFoundError)?;

        if record.is_too_big {
            return Err(
                io::Error::new(io::ErrorKind::Other, crate::error::SizeLimitReached).into(),
            );
        }

        let compression = record.compression.map(|i| {
            i.try_into()
                .expect("invalid compression algorithm stored in database")
        });
        Ok(Blob {
            path: record.path,
            mime: record.mime,
            date_updated: DateTime::from_utc(record.date_updated, Utc),
            content: record.content.expect("size errors were handled above"),
            compression,
        })
    }

    pub(super) fn start_connection(&self) -> Result<DatabaseClient, Error> {
        Ok(DatabaseClient {
            conn: self.pool.get()?,
            metrics: self.metrics.clone(),
        })
    }
}

pub(super) struct DatabaseClient {
    conn: crate::db::Client,
    metrics: Arc<Metrics>,
}

impl DatabaseClient {
    pub(super) fn start_storage_transaction(
        &mut self,
    ) -> Result<DatabaseStorageTransaction<'_>, Error> {
        Ok(DatabaseStorageTransaction {
            transaction: self.conn.begin().block()?,
            metrics: &self.metrics,
        })
    }
}

pub(super) struct DatabaseStorageTransaction<'a> {
    transaction: Transaction<'a, sqlx::Postgres>,
    metrics: &'a Metrics,
}

impl<'a> StorageTransaction for DatabaseStorageTransaction<'a> {
    fn store_batch(&mut self, batch: Vec<Blob>) -> Result<(), Error> {
        for blob in batch {
            let compression = blob.compression.map(|alg| alg as i32);
            query!(
                "INSERT INTO files (path, mime, content, compression)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (path) DO UPDATE
                    SET mime = EXCLUDED.mime, content = EXCLUDED.content, compression = EXCLUDED.compression",
                blob.path, blob.mime, blob.content, compression,
            ).execute(&mut self.transaction).block()?;
            self.metrics.uploaded_files_total.inc();
        }
        Ok(())
    }

    fn delete_prefix(&mut self, prefix: &str) -> Result<(), Error> {
        query!(
            "DELETE FROM files WHERE path LIKE $1;",
            format!("{}%", prefix.replace('%', "\\%")),
        )
        .execute(&mut self.transaction)
        .block()?;
        Ok(())
    }

    fn complete(self: Box<Self>) -> Result<(), Error> {
        self.transaction.commit().block().map_err(Into::into)
    }
}

// The tests for this module are in src/storage/mod.rs, as part of the backend tests. Please add
// any test checking the public interface there.
