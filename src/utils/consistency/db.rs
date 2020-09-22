use super::data::{Crate, CrateName, Data, Release, Version};
use crate::db::Client;
use crate::Blocking;
use futures_util::StreamExt;
use sqlx::query;
use std::collections::BTreeMap;

pub(crate) fn load(conn: &mut Client) -> Result<Data, failure::Error> {
    let mut rows = query!(
        "
        SELECT
            crates.name,
            releases.version
        FROM crates
        INNER JOIN releases ON releases.crate_id = crates.id
        ORDER BY crates.id, releases.id
    ",
    )
    .fetch(conn);

    let mut data = Data {
        crates: BTreeMap::new(),
    };

    struct Current {
        name: CrateName,
        krate: Crate,
    }

    let mut current = if let Some(row) = rows.next().block().transpose()? {
        Current {
            name: CrateName(row.name),
            krate: Crate {
                releases: {
                    let mut releases = BTreeMap::new();
                    releases.insert(Version(row.version), Release {});
                    releases
                },
            },
        }
    } else {
        return Ok(data);
    };

    while let Some(row) = rows.next().block().transpose()? {
        let name = row.name;
        if current.name != name {
            data.crates.insert(
                std::mem::replace(&mut current.name, CrateName(name)),
                std::mem::take(&mut current.krate),
            );
        }
        current
            .krate
            .releases
            .insert(Version(row.version), Release::default());
    }

    data.crates.insert(current.name, current.krate);

    Ok(data)
}
