use crate::{CacheManager, HttpResponse, Result};

use http_cache_semantics::CachePolicy;
use serde::{Deserialize, Serialize};
use url::Url;

/// Implements [`CacheManager`] with [`rusqlite`](https://github.com/rusqlite/rusqlite) as the backend.
#[cfg_attr(docsrs, doc(cfg(feature = "manager-sqlite")))]
#[derive(Debug, Clone)]
pub struct SqliteManager {
    /// Directory where the cache will be stored.
    pub path: String,
    connection: tokio_rusqlite::Connection,
}

#[derive(Debug, Deserialize, Serialize)]
struct Store {
    response: HttpResponse,
    policy: CachePolicy,
}

fn req_key(method: &str, url: &Url) -> String {
    format!("{method}:{url}")
}

#[allow(dead_code)]
impl SqliteManager {
    /// Creates a new [`SqliteManager`] at given path.
    pub async fn new(path: &str) -> Result<Self> {
        let manager = Self {
            path: path.into(),
            connection: tokio_rusqlite::Connection::open(path).await?,
        };
        manager.create_tables().await?;
        Ok(manager)
    }

    /// Creates a new [`SqliteManager`] in memory.
    pub async fn new_in_memory() -> Result<Self> {
        let path = ":memory:";
        let manager = Self {
            path: path.into(),
            connection: tokio_rusqlite::Connection::open_in_memory().await?,
        };
        manager.create_tables().await?;
        Ok(manager)
    }

    /// Creates a new [`SqliteManager`] at the default path `./http-sqlite.db`.
    pub async fn new_default() -> Result<Self> {
        let path = "./http-sqlite.db";
        Self::new(path).await
    }

    async fn create_tables(&self) -> Result<()> {
        self.connection
            .call(|connection| {
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS cache (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        req_key TEXT NOT NULL,
                        store BLOB NOT NULL,
                        UNIQUE(req_key)
                    )",
                    (),
                )?;
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS cache_req_key_idx ON cache (req_key)",
                    (),
                )?;

                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn write(&self, req_key: String, store: &Store) -> Result<()> {
        let bytes = bincode::serialize(store)?;
        self.connection
            .call(move |connection| {
                connection.execute(
                "INSERT OR REPLACE INTO cache (req_key, store) VALUES (?1, ?2)",
            (&req_key, &bytes),
                )?;
                Ok(())
            })
            .await?;

        Ok(())
    }

    async fn read(&self, req_key: String) -> Result<Option<Store>> {
        Ok(self
            .connection
            .call(move |connection| {
                let mut stmt = connection
                    .prepare("SELECT store FROM cache WHERE req_key = ?1")?;
                let mut rows = stmt.query([&req_key])?;
                if let Some(row) = rows.next()? {
                    let bytes: Vec<u8> = row.get(0)?;
                    if let Ok(desialized) = bincode::deserialize(&bytes) {
                        Ok(Some(desialized))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            })
            .await?)
    }

    async fn delete(&self, req_key: String) -> Result<()> {
        self.connection
            .call(move |connection| {
                connection.execute(
                    "DELETE FROM cache WHERE req_key = ?1",
                    [&req_key],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Clears out the entire cache.
    pub async fn clear(&self) -> Result<()> {
        self.connection
            .call(|connection| {
                connection.execute("DELETE FROM cache", ())?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CacheManager for SqliteManager {
    async fn get(
        &self,
        method: &str,
        url: &Url,
    ) -> Result<Option<(HttpResponse, CachePolicy)>> {
        let store: Store = match self.read(req_key(method, url)).await? {
            Some(store) => store,
            None => return Ok(None),
        };
        Ok(Some((store.response, store.policy)))
    }

    async fn put(
        &self,
        method: &str,
        url: &Url,
        response: HttpResponse,
        policy: CachePolicy,
    ) -> Result<HttpResponse> {
        let data = Store { response: response.clone(), policy };
        self.write(req_key(method, url), &data).await?;
        Ok(response)
    }

    async fn delete(&self, method: &str, url: &Url) -> Result<()> {
        Ok(self.delete(req_key(method, url)).await?)
    }
}
