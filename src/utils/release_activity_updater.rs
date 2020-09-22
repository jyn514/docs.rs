use crate::db::Client;
use crate::error::Result;
use crate::Blocking;
use chrono::{Duration, Utc};
use serde_json::{Map, Value};
use sqlx::query;

pub fn update_release_activity(conn: &mut Client) -> Result<()> {
    let mut dates = Vec::with_capacity(30);
    let mut crate_counts = Vec::with_capacity(30);
    let mut failure_counts = Vec::with_capacity(30);

    // TODO: use async here instead of blocking on queries in a loop
    for day in 0..30 {
        let release_count = query!(
            r#"SELECT COUNT(*) as "count!"
                FROM releases
                WHERE release_time < NOW() - CONCAT($1::text, ' day')::INTERVAL AND
                      release_time > NOW() - CONCAT($2::text, ' day')::INTERVAL"#,
            day.to_string(),
            (day + 1).to_string(),
        )
        .fetch_one(&mut *conn)
        .block()?
        .count;
        let failures_count_rows = query!(
            r#"SELECT COUNT(*) as "count!"
                FROM releases
                WHERE is_library = TRUE AND
                    build_status = FALSE AND
                    release_time < NOW() - CONCAT($1::text, ' day')::INTERVAL AND
                    release_time > NOW() - CONCAT($2::text, ' day')::INTERVAL"#,
            day.to_string(),
            (day + 1).to_string(),
        )
        .fetch_one(&mut *conn);

        let now = Utc::now().naive_utc();
        let date = now - Duration::days(day);
        dates.push(format!("{}", date.format("%d %b")));

        let failure_count: i64 = failures_count_rows.block()?.count;
        crate_counts.push(release_count);
        failure_counts.push(failure_count);
    }

    dates.reverse();
    crate_counts.reverse();
    failure_counts.reverse();

    let map = {
        let mut map = Map::new();
        map.insert("dates".to_owned(), serde_json::to_value(dates)?);
        map.insert("counts".to_owned(), serde_json::to_value(crate_counts)?);
        map.insert("failures".to_owned(), serde_json::to_value(failure_counts)?);

        Value::Object(map)
    };

    query!(
        "INSERT INTO config (name, value) VALUES ('release_activity', $1)
         ON CONFLICT (name) DO UPDATE
            SET value = $1 WHERE config.name = 'release_activity'",
        map,
    )
    .execute(conn)
    .block()?;

    Ok(())
}
