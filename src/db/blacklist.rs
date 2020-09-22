use crate::db::Client;
use crate::Blocking;
use failure::{Error, Fail};
use futures_util::TryStreamExt;
use sqlx::query;

#[derive(Debug, Fail)]
enum BlacklistError {
    #[fail(display = "crate {} is already on the blacklist", _0)]
    CrateAlreadyOnBlacklist(String),

    #[fail(display = "crate {} is not on the blacklist", _0)]
    CrateNotOnBlacklist(String),
}

/// Returns whether the given name is blacklisted.
pub fn is_blacklisted(conn: &mut Client, name: &str) -> Result<bool, Error> {
    let count = query!(
        // postgres can't infer nullability from expressions; this should never be NULL
        // the `count!` tells SQLx to give a runtime error if it's ever NULL
        r#"SELECT COUNT(*) as "count!" FROM blacklisted_crates WHERE crate_name = $1;"#,
        name,
    )
    .fetch_one(conn)
    .block()?
    .count;

    Ok(count != 0)
}

/// Returns the crate names on the blacklist, sorted ascending.
pub fn list_crates(conn: &mut Client) -> Result<Vec<String>, Error> {
    query!("SELECT crate_name FROM blacklisted_crates ORDER BY crate_name asc;")
        .fetch(conn)
        .map_ok(|record| record.crate_name)
        .try_collect()
        .block()
        .map_err(Into::into)
}

/// Adds a crate to the blacklist.
pub fn add_crate(conn: &mut Client, name: &str) -> Result<(), Error> {
    if is_blacklisted(conn, name)? {
        return Err(BlacklistError::CrateAlreadyOnBlacklist(name.into()).into());
    }

    query!(
        "INSERT INTO blacklisted_crates (crate_name) VALUES ($1);",
        name,
    )
    .execute(conn)
    .block()?;

    Ok(())
}

/// Removes a crate from the blacklist.
pub fn remove_crate(conn: &mut Client, name: &str) -> Result<(), Error> {
    if !is_blacklisted(conn, name)? {
        return Err(BlacklistError::CrateNotOnBlacklist(name.into()).into());
    }

    query!(
        "DELETE FROM blacklisted_crates WHERE crate_name = $1;",
        name,
    )
    .execute(conn)
    .block()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_blacklist() {
        crate::test::wrapper(|env| {
            let db = env.db();

            // crates are added out of order to verify sorting
            add_crate(&mut db.conn(), "crate A")?;
            add_crate(&mut db.conn(), "crate C")?;
            add_crate(&mut db.conn(), "crate B")?;

            assert!(list_crates(&mut db.conn())? == vec!["crate A", "crate B", "crate C"]);
            Ok(())
        });
    }

    #[test]
    fn test_add_to_and_remove_from_blacklist() {
        crate::test::wrapper(|env| {
            let db = env.db();

            assert!(!is_blacklisted(&mut db.conn(), "crate foo")?);
            add_crate(&mut db.conn(), "crate foo")?;
            assert!(is_blacklisted(&mut db.conn(), "crate foo")?);
            remove_crate(&mut db.conn(), "crate foo")?;
            assert!(!is_blacklisted(&mut db.conn(), "crate foo")?);
            Ok(())
        });
    }

    #[test]
    fn test_add_twice_to_blacklist() {
        crate::test::wrapper(|env| {
            let db = env.db();

            add_crate(&mut db.conn(), "crate foo")?;
            assert!(add_crate(&mut db.conn(), "crate foo").is_err());
            add_crate(&mut db.conn(), "crate bar")?;

            Ok(())
        });
    }

    #[test]
    fn test_remove_non_existing_crate() {
        crate::test::wrapper(|env| {
            let db = env.db();

            assert!(remove_crate(&mut db.conn(), "crate foo").is_err());

            Ok(())
        });
    }
}
