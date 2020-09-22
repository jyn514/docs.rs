use crate::db::Client;
use crate::{Blocking, Storage};
use failure::{Error, Fail};
use futures_util::TryFutureExt;
use sqlx::{query, Connection, Executor};

/// List of directories in docs.rs's underlying storage (either the database or S3) containing a
/// subdirectory named after the crate. Those subdirectories will be deleted.
static STORAGE_PATHS_TO_DELETE: &[&str] = &["rustdoc", "sources"];

#[derive(Debug, Fail)]
enum CrateDeletionError {
    #[fail(display = "crate is missing: {}", _0)]
    MissingCrate(String),
}

pub fn delete_crate(conn: &mut Client, storage: &Storage, name: &str) -> Result<(), Error> {
    let crate_id = get_id(conn, name)?;
    delete_crate_from_database(conn, name, crate_id)?;

    for prefix in STORAGE_PATHS_TO_DELETE {
        storage.delete_prefix(&format!("{}/{}/", prefix, name))?;
    }

    Ok(())
}

pub fn delete_version(
    conn: &mut Client,
    storage: &Storage,
    name: &str,
    version: &str,
) -> Result<(), Error> {
    delete_version_from_database(conn, name, version)?;

    for prefix in STORAGE_PATHS_TO_DELETE {
        storage.delete_prefix(&format!("{}/{}/{}/", prefix, name, version))?;
    }

    Ok(())
}

fn get_id(conn: &mut Client, name: &str) -> Result<i32, Error> {
    let rec = query!("SELECT id FROM crates WHERE name = $1", name)
        .fetch_optional(conn)
        .block()?;
    if let Some(rec) = rec {
        Ok(rec.id)
    } else {
        Err(CrateDeletionError::MissingCrate(name.into()).into())
    }
}

// metaprogramming!
// WARNING: these must be hard-coded and NEVER user input.
const METADATA: &[(&str, &str)] = &[
    ("author_rels", "rid"),
    ("keyword_rels", "rid"),
    ("builds", "rid"),
    ("compression_rels", "release"),
    ("doc_coverage", "release_id"),
];

fn delete_version_from_database(conn: &mut Client, name: &str, version: &str) -> Result<(), Error> {
    let crate_id = get_id(conn, name)?;
    let mut transaction = conn.begin().block()?;
    for &(table, column) in METADATA {
        // TODO: don't `block()` on individual statements, just the whole transaction
        transaction.execute(
            sqlx::query(
                    format!("DELETE FROM {} WHERE {} IN (SELECT id FROM releases WHERE crate_id = $1 AND version = $2)", table, column).as_str(),
                ).bind(crate_id).bind(version)
        )
        .block()?;
    }
    query!(
        "DELETE FROM releases WHERE crate_id = $1 AND version = $2",
        crate_id,
        version,
    )
    .execute(&mut transaction)
    .block()?;
    query!(
        "UPDATE crates SET latest_version_id = (
            SELECT id FROM releases WHERE release_time = (
                SELECT MAX(release_time) FROM releases WHERE crate_id = $1
            )
        ) WHERE id = $1",
        crate_id,
    )
    .execute(&mut transaction)
    .block()?;

    for prefix in STORAGE_PATHS_TO_DELETE {
        query!(
            "DELETE FROM files WHERE path LIKE $1;",
            format!("{}/{}/{}/%", prefix, name, version),
        )
        .execute(&mut transaction)
        .block()?;
    }

    transaction.commit().map_err(Into::into).block()
}

fn delete_crate_from_database(conn: &mut Client, name: &str, crate_id: i32) -> Result<(), Error> {
    let mut transaction = conn.begin().block()?;

    query!("DELETE FROM sandbox_overrides WHERE crate_name = $1", name,)
        .execute(&mut transaction)
        .block()?;

    for &(table, column) in METADATA {
        transaction
            .execute(
                sqlx::query(&format!(
                    "DELETE FROM {} WHERE {} IN (SELECT id FROM releases WHERE crate_id = $1)",
                    table, column
                ))
                .bind(crate_id),
            )
            .block()?;
    }
    query!("DELETE FROM owner_rels WHERE cid = $1;", crate_id)
        .execute(&mut transaction)
        .block()?;
    query!("DELETE FROM releases WHERE crate_id = $1;", crate_id)
        .execute(&mut transaction)
        .block()?;
    query!("DELETE FROM crates WHERE id = $1;", crate_id)
        .execute(&mut transaction)
        .block()?;

    // Transactions automatically rollback when not committing, so if any of the previous queries
    // fail the whole transaction will be aborted.
    transaction.commit().block()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Client;
    use crate::test::{assert_success, wrapper};
    use failure::Error;

    fn crate_exists(conn: &mut Client, name: &str) -> Result<bool, Error> {
        Ok(query!(
            r#"SELECT COUNT(*) as "count!" FROM crates WHERE name = $1;"#,
            name
        )
        .fetch_one(conn)
        .block()?
        .count
            > 0)
    }

    fn release_exists(conn: &mut Client, id: i32) -> Result<bool, Error> {
        Ok(query!(
            r#"SELECT COUNT(*) as "count!" FROM releases WHERE id = $1;"#,
            id
        )
        .fetch_one(conn)
        .block()?
        .count
            > 0)
    }

    #[test]
    fn test_delete_from_database() {
        wrapper(|env| {
            let db = env.db();

            // Create fake packages in the database
            let pkg1_v1_id = env
                .fake_release()
                .name("package-1")
                .version("1.0.0")
                .create()?;
            let pkg1_v2_id = env
                .fake_release()
                .name("package-1")
                .version("2.0.0")
                .create()?;
            let pkg2_id = env.fake_release().name("package-2").create()?;

            assert!(crate_exists(&mut db.conn(), "package-1")?);
            assert!(crate_exists(&mut db.conn(), "package-2")?);
            assert!(release_exists(&mut db.conn(), pkg1_v1_id)?);
            assert!(release_exists(&mut db.conn(), pkg1_v2_id)?);
            assert!(release_exists(&mut db.conn(), pkg2_id)?);

            let pkg1_id = query!("SELECT id FROM crates WHERE name = 'package-1';")
                .fetch_one(&mut db.conn())
                .block()?
                .id;

            delete_crate_from_database(&mut db.conn(), "package-1", pkg1_id)?;

            assert!(!crate_exists(&mut db.conn(), "package-1")?);
            assert!(crate_exists(&mut db.conn(), "package-2")?);
            assert!(!release_exists(&mut db.conn(), pkg1_v1_id)?);
            assert!(!release_exists(&mut db.conn(), pkg1_v2_id)?);
            assert!(release_exists(&mut db.conn(), pkg2_id)?);

            Ok(())
        });
    }

    #[test]
    fn test_delete_version() {
        wrapper(|env| {
            fn authors(conn: &mut Client, crate_id: i32) -> Result<Vec<String>, Error> {
                Ok(query!(
                    "SELECT name FROM authors
                        INNER JOIN author_rels ON authors.id = author_rels.aid
                        INNER JOIN releases ON author_rels.rid = releases.id
                    WHERE releases.crate_id = $1",
                    crate_id,
                )
                .fetch_all(conn)
                .block()?
                .into_iter()
                .map(|row| row.name)
                .collect())
            }

            let db = env.db();
            let v1 = env
                .fake_release()
                .name("a")
                .version("1.0.0")
                .author("malicious actor")
                .create()?;
            let v2 = env
                .fake_release()
                .name("a")
                .version("2.0.0")
                .author("Peter Rabbit")
                .create()?;
            assert!(release_exists(&mut db.conn(), v1)?);
            assert!(release_exists(&mut db.conn(), v2)?);
            let crate_id = query!("SELECT crate_id FROM releases WHERE id = $1", v1)
                .fetch_one(&mut db.conn())
                .block()?
                .crate_id;
            assert_eq!(
                authors(&mut db.conn(), crate_id)?,
                vec!["malicious actor".to_string(), "Peter Rabbit".to_string()]
            );

            delete_version(&mut db.conn(), &*env.storage(), "a", "1.0.0")?;
            assert!(!release_exists(&mut db.conn(), v1)?);
            assert!(release_exists(&mut db.conn(), v2)?);
            assert_eq!(
                authors(&mut db.conn(), crate_id)?,
                vec!["Peter Rabbit".to_string()]
            );

            let web = env.frontend();
            assert_success("/a/2.0.0/a/", web)?;
            assert_eq!(web.get("/a/1.0.0/a/").send()?.status(), 404);

            Ok(())
        })
    }
}
