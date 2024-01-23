use super::context::ServerContext;
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
};
use bytes::{Bytes, BytesMut};
use cached::{proc_macro::cached, Cached};
use futures::StreamExt;
use md5::{Digest, Md5};
use tracing::{instrument, Instrument};

const PATH_REGEX: &str = r"^(?:[[:alnum:]\.~_-]+)(?:/[[:alnum:]\.~_-]+)*$";
const BUILD_ASSET_QUERY: &str ="
WITH
  asset_tables AS (
    SELECT
      c.oid, c.relname, n.nspname
    FROM
      pg_class c
      INNER JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE relkind = 'r' AND obj_description(c.oid) = '@assets'
  ),
  asset_table_columns AS (
    SELECT
      a.attrelid, a.attname, t.typname
    FROM
      pg_attribute a
      INNER JOIN pg_type t on a.atttypid = t.oid
    WHERE attrelid IN (SELECT oid FROM asset_tables)
  ),
  asset_table_queries AS (
    SELECT
      'SELECT name, data, ' || (CASE WHEN EXISTS (SELECT 1 FROM asset_table_columns WHERE attrelid = oid AND attname = 'hash' AND typname = 'varchar') THEN 'hash' ELSE 'md5(data)::varchar AS hash' END) || ', ' || (CASE WHEN EXISTS (SELECT 1 FROM asset_table_columns WHERE attrelid = oid AND attname = 'modified' AND typname = 'timestamptz') THEN 'modified' ELSE 'now() AS modified' END) || ' FROM ' || nspname || '.' || relname AS sql
    FROM
      asset_tables
    WHERE
      exists (select 1 from asset_table_columns where attrelid = oid and attname = 'name' and typname = 'varchar') AND exists (select 1 from asset_table_columns where attrelid = oid and attname = 'data' and typname = 'bytea')
  )
 SELECT string_agg('(' || sql || ')', ' UNION ') FROM asset_table_queries
";

#[instrument(skip(state))]
pub async fn asset_path_handler(
    State(mut state): State<ServerContext>,
    Path(path): Path<String>,
) -> axum::response::Response {
    let path_regex = regex::Regex::new(PATH_REGEX).expect("Expected the const regex to be valid");
    if path_regex.is_match(path.as_str()) {
        get_asset_response(&mut state, &path).await
    } else {
        (StatusCode::BAD_REQUEST).into_response()
    }
}

#[instrument(skip(pool))]
async fn get_copy_out_query_data(
    pool: &sqlx::Pool<sqlx::Postgres>,
    sql: &str,
) -> Result<Bytes, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    let mut stream = connection
        .copy_out_raw(sql)
        .instrument(tracing::debug_span!("query_asset"))
        .await?;
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut error: Option<sqlx::Error> = None;
    while let Some(result) = stream
        .next()
        .instrument(tracing::debug_span!("stream_next"))
        .await
    {
        match result {
            Ok(bytes) => buffer.extend_from_slice(&bytes),
            Err(e) => error = Some(e),
        }
    }
    if let Some(e) = error {
        Err(e)
    } else {
        Ok(buffer.freeze())
    }
}

#[instrument(skip(state))]
pub async fn get_asset_response(state: &mut ServerContext, path: &str) -> axum::response::Response {
    if let Some(sql) = generate_asset_query_for_path(state, path).await {
        if let Ok(buffer) = get_copy_out_query_data(&state.pool, &sql).await {
            return asset_query_copy_out_buffer_to_response(buffer);
        } else {
            // The state contained an asset query that did not execute
            // correctly, maybe the database schema changed since we built that
            // query. So we clear the cache and try again once for this asset.
            {
                let mut cache = EXTRACT_ASSET_QUERY_FROM_DATABASE.lock().await;
                let key = get_pool_cache_key(&state.pool);
                cache.cache_remove(&key);
            }

            if let Some(sql) = generate_asset_query_for_path(state, path).await {
                if let Ok(buffer) = get_copy_out_query_data(&state.pool, &sql).await {
                    return asset_query_copy_out_buffer_to_response(buffer);
                }
            }
        }
    }

    tracing::warn!(
        "Failed to query assets because this database does not seem to contain any asset tables."
    );
    (StatusCode::SERVICE_UNAVAILABLE).into_response()
}

#[instrument(skip(state))]
async fn generate_asset_query_for_path(state: &mut ServerContext, path: &str) -> Option<String> {
    if let Ok(sql) = extract_asset_query_from_database(&state.pool).await {
        return Some(format!("COPY (WITH assets as ({}) SELECT name, hash, to_char(modified::timestamp at time zone 'UTC', 'Dy, DD Mon YYYY HH:MI:SS' || ' GMT') as modified, data FROM assets WHERE name='{}' LIMIT 1) TO STDOUT WITH BINARY", sql, path));
    }
    tracing::warn!("Failed to generate query for retrieving assets from the database.");
    None
}

#[cached(
    time = 14400,
    key = "String",
    convert = r#"{ get_pool_cache_key(pool) }"#,
    result = true
)]
async fn extract_asset_query_from_database(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(BUILD_ASSET_QUERY)
        .fetch_one(pool)
        .instrument(tracing::debug_span!("extract_asset_query_from_database"))
        .await
}

fn get_pool_cache_key(pool: &sqlx::Pool<sqlx::Postgres>) -> String {
    let str = format!("{:?}", pool.connect_options());
    let mut hasher = Md5::new();
    hasher.update(str.as_bytes());
    hex::encode(hasher.finalize())
}

fn asset_query_copy_out_buffer_to_response(buffer: Bytes) -> axum::response::Response {
    let parser_result = parser::parse_asset_stream(&buffer[..]);
    return match parser_result {
        Ok(asset) => {
            let content_type = mime_guess::from_path(asset.name.as_str())
                .first_or(mime_guess::mime::APPLICATION_OCTET_STREAM)
                .to_string();
            tracing::debug!("Found asset {} of {} bytes", asset.name, asset.body.len());
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::ETAG, asset.hash),
                    (header::CONTENT_LENGTH, asset.body.len().to_string()),
                ],
                buffer.slice(asset.body),
            )
                .into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND).into_response(),
    };
}

mod parser {
    use combine::{
        any, parser::byte::bytes, parser::byte::num::be_i32, parser::range::take,
        parser::repeat::skip_count, parser::token::position, ParseError, Parser, RangeStream,
    };

    const HEADER_MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
    //const EOS_MARKER: &[u8] = b"\xff\xff";

    #[derive(Clone, Debug)]
    pub struct Asset {
        pub name: String,
        pub hash: String,
        pub modified: String,
        pub body: std::ops::Range<usize>,
    }

    fn make_asset_stream_parser<'a, I>() -> impl Parser<I, Output = Asset> + 'a
    where
        I: RangeStream<Token = u8, Range = &'a [u8], Position = usize> + 'a,
        I::Error: ParseError<I::Token, I::Range, I::Position>,
    {
        let magic = bytes(HEADER_MAGIC);
        let flags = be_i32();
        let extension = be_i32().then(|n| skip_count(n.try_into().unwrap(), any()));
        let field_count = bytes(b"\x00\x04"); // 4 columns
        let name_field = be_i32().then(|n| take(n.try_into().unwrap()));
        let hash_field = be_i32().then(|n| take(n.try_into().unwrap()));
        let modified_field = be_i32().then(|n| take(n.try_into().unwrap()));
        let body_field = be_i32().then(|n| {
            (
                position(),
                skip_count(n.try_into().unwrap(), any()).with(position()),
            )
        });

        (magic, flags, extension)
            .with(field_count.with((name_field, hash_field, modified_field, body_field)))
            .map(|data| Asset {
                name: std::str::from_utf8(data.0)
                    .expect("Expected an UTF8 encoded name")
                    .to_string(),
                hash: std::str::from_utf8(data.1)
                    .expect("Expected hash to be UTF8 encoded")
                    .to_string(),
                modified: std::str::from_utf8(data.2)
                    .expect("Expected modified to be UTF8 encoded")
                    .to_string(),
                body: std::ops::Range {
                    start: data.3 .0,
                    end: data.3 .1,
                },
            })
    }

    #[instrument(skip(buffer), ret, err)]
    pub fn parse_asset_stream(buffer: &[u8]) -> Result<Asset, combine::error::UnexpectedParse> {
        let stream = combine::stream::position::Stream::new(buffer);
        make_asset_stream_parser()
            .parse(stream)
            .map(|result| result.0)
    }

    #[cfg(test)]
    use speculoos::{assert_that, result::ResultAssertions};
    use tracing::instrument;

    #[test]
    fn parse_copy_out_data_with_asset() {
        // Arrange
        let data = include_bytes!("./test_data/test_txt_copy_out_query.data");

        // Act
        let result = parse_asset_stream(&data[..]);

        // Assert
        assert_that(&result).is_ok();
        let asset = result.unwrap();
        assert_that(&asset.name.as_str()).is_equal_to("test.txt");
        assert_that(&asset.hash.as_str()).is_equal_to("d8e8fca2dc0f896fd7cb4cb0031ba249");
        assert_that(&asset.body.len()).is_equal_to(5);
        assert_that(&&data[asset.body]).is_equal_to("test\n".as_bytes());
    }

    #[test]
    fn parse_copy_out_data_without_asset() {
        // Arrange
        let data = include_bytes!("./test_data/no_result_copy_out_query.data");

        // Act
        let result = parse_asset_stream(&data[..]);

        // Assert
        assert_that(&result).is_err();
    }
}
