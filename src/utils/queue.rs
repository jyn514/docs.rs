//! Utilities for interacting with the build queue

use crate::db::Client;
use crate::error::Result;
use crate::Blocking;
use sqlx::query;

const DEFAULT_PRIORITY: i32 = 0;

/// Get the build queue priority for a crate
pub fn get_crate_priority(conn: &mut Client, name: &str) -> Result<i32> {
    // Search the `priority` table for a priority where the crate name matches the stored pattern
    let row = query!(
        "SELECT priority FROM crate_priorities WHERE $1 LIKE pattern LIMIT 1",
        name,
    )
    .fetch_optional(conn)
    .block()?;

    // If no match is found, return the default priority
    if let Some(row) = row {
        Ok(row.priority)
    } else {
        Ok(DEFAULT_PRIORITY)
    }
}

/// Set all crates that match [`pattern`] to have a certain priority
///
/// Note: `pattern` is used in a `LIKE` statement, so it must follow the postgres like syntax
///
/// [`pattern`]: https://www.postgresql.org/docs/8.3/functions-matching.html
pub fn set_crate_priority(conn: &mut Client, pattern: &str, priority: i32) -> Result<()> {
    query!(
        "INSERT INTO crate_priorities (pattern, priority) VALUES ($1, $2)",
        pattern,
        priority,
    )
    .execute(conn)
    .block()?;

    Ok(())
}

/// Remove a pattern from the priority table, returning the priority that it was associated with or `None`
/// if nothing was removed
pub fn remove_crate_priority(conn: &mut Client, pattern: &str) -> Result<Option<i32>> {
    let priority = query!(
        "DELETE FROM crate_priorities WHERE pattern = $1 RETURNING priority",
        pattern,
    )
    .fetch_optional(conn)
    .block()?
    .map(|row| row.priority);

    Ok(priority)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::wrapper;

    #[test]
    fn set_priority() {
        wrapper(|env| {
            let db = env.db();

            set_crate_priority(&mut db.conn(), "cratesfyi-%", -100)?;
            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi-database")?,
                -100
            );
            assert_eq!(get_crate_priority(&mut db.conn(), "cratesfyi-")?, -100);
            assert_eq!(get_crate_priority(&mut db.conn(), "cratesfyi-s3")?, -100);
            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi-webserver")?,
                -100
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi")?,
                DEFAULT_PRIORITY
            );

            set_crate_priority(&mut db.conn(), "_c_", 100)?;
            assert_eq!(get_crate_priority(&mut db.conn(), "rcc")?, 100);
            assert_eq!(get_crate_priority(&mut db.conn(), "rc")?, DEFAULT_PRIORITY);

            set_crate_priority(&mut db.conn(), "hexponent", 10)?;
            assert_eq!(get_crate_priority(&mut db.conn(), "hexponent")?, 10);
            assert_eq!(
                get_crate_priority(&mut db.conn(), "hexponents")?,
                DEFAULT_PRIORITY
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "floathexponent")?,
                DEFAULT_PRIORITY
            );

            Ok(())
        })
    }

    #[test]
    fn remove_priority() {
        wrapper(|env| {
            let db = env.db();

            set_crate_priority(&mut db.conn(), "cratesfyi-%", -100)?;
            assert_eq!(get_crate_priority(&mut db.conn(), "cratesfyi-")?, -100);

            assert_eq!(
                remove_crate_priority(&mut db.conn(), "cratesfyi-%")?,
                Some(-100)
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi-")?,
                DEFAULT_PRIORITY
            );

            Ok(())
        })
    }

    #[test]
    fn get_priority() {
        wrapper(|env| {
            let db = env.db();

            set_crate_priority(&mut db.conn(), "cratesfyi-%", -100)?;

            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi-database")?,
                -100
            );
            assert_eq!(get_crate_priority(&mut db.conn(), "cratesfyi-")?, -100);
            assert_eq!(get_crate_priority(&mut db.conn(), "cratesfyi-s3")?, -100);
            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi-webserver")?,
                -100
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "unrelated")?,
                DEFAULT_PRIORITY
            );

            Ok(())
        })
    }

    #[test]
    fn get_default_priority() {
        wrapper(|env| {
            let db = env.db();

            assert_eq!(
                get_crate_priority(&mut db.conn(), "cratesfyi")?,
                DEFAULT_PRIORITY
            );
            assert_eq!(get_crate_priority(&mut db.conn(), "rcc")?, DEFAULT_PRIORITY);
            assert_eq!(
                get_crate_priority(&mut db.conn(), "lasso")?,
                DEFAULT_PRIORITY
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "hexponent")?,
                DEFAULT_PRIORITY
            );
            assert_eq!(
                get_crate_priority(&mut db.conn(), "rust4lyfe")?,
                DEFAULT_PRIORITY
            );

            Ok(())
        })
    }
}
