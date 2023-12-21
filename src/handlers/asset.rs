use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
};
use bytes::{Bytes, BytesMut};
use combine::{stream::position::Stream, Parser};
use futures::StreamExt;

use super::context::ServerContext;

pub async fn asset_path_handler(
    State(state): State<ServerContext>,
    Path(path): Path<String>,
) -> axum::response::Response {
    let path_regex = regex::Regex::new(PATH_REGEX).expect("Expected the const regex to be valid");
    if path_regex.is_match(path.as_str()) {
        get_asset_response(&state.pool, &path).await
    } else {
        (StatusCode::BAD_REQUEST).into_response()
    }
}

async fn get_asset_query_copy_out_data(
    pool: &sqlx::Pool<sqlx::Postgres>,
    path: &str,
) -> Result<Bytes, sqlx::Error> {
    let sql = format!("copy (select name, md5(data), data from assets where name='{}' limit 1) to stdout with binary;", path);
    let mut connection = pool.acquire().await?;
    let mut stream = connection.copy_out_raw(sql.as_str()).await?;
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut error: Option<sqlx::Error> = None;
    while let Some(result) = stream.next().await {
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

pub async fn get_asset_response(
    pool: &sqlx::Pool<sqlx::Postgres>,
    path: &str,
) -> axum::response::Response {
    match get_asset_query_copy_out_data(pool, path).await {
        Ok(buffer) => {
            let content_type = mime_guess::from_path(path)
                .first_or(mime_guess::mime::APPLICATION_OCTET_STREAM)
                .to_string();
            let parser_result = parser::parse_asset_stream().parse(Stream::new(&buffer[..]));
            match parser_result {
                Ok((asset, _)) => (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, content_type),
                        (header::ETAG, asset.hash),
                        (header::CONTENT_LENGTH, asset.body.len().to_string()),
                    ],
                    buffer.slice(asset.body),
                )
                    .into_response(),

                Err(_) => (StatusCode::NOT_FOUND).into_response(),
            }
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE).into_response(),
    }
}

const PATH_REGEX: &str = r"^(?:[[:alnum:]\.~_-]+)(?:/[[:alnum:]\.~_-]+)*$";

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
        pub body: std::ops::Range<usize>,
    }

    pub fn parse_asset_stream<'a, I>() -> impl Parser<I, Output = Asset> + 'a
    where
        I: RangeStream<Token = u8, Range = &'a [u8], Position = usize> + 'a,
        I::Error: ParseError<I::Token, I::Range, I::Position>,
    {
        let magic = bytes(HEADER_MAGIC);
        let flags = be_i32();
        let extension = be_i32().then(|n| skip_count(n.try_into().unwrap(), any()));
        let field_count = bytes(b"\x00\x03");
        let name_field = be_i32().then(|n| take(n.try_into().unwrap()));
        let hash_field = be_i32().then(|n| take(n.try_into().unwrap()));
        let body_field = be_i32().then(|n| {
            (
                position(),
                skip_count(n.try_into().unwrap(), any()).with(position()),
            )
        });

        (magic, flags, extension)
            .with(field_count.with((name_field, hash_field, body_field)))
            .map(|data| Asset {
                name: std::str::from_utf8(data.0)
                    .expect("Expected an UTF8 encoded name")
                    .to_string(),
                hash: std::str::from_utf8(data.1)
                    .expect("Expected hash to be UTF8 encoded")
                    .to_string(),
                body: std::ops::Range {
                    start: data.2 .0,
                    end: data.2 .1,
                },
            })
    }

    // #[test]
    // fn header_test() {
    //     let data = include_bytes!("../assets/copy-out-data");
    //     println!("DATA: {:?}", data);
    //     let result = decode(data);
    //     if let Ok(tuples) = result {
    //         for tuple in tuples {
    //             println!(
    //                 "{} | {:?}",
    //                 //u32::from_be_bytes(tuple.data[0].try_into()),
    //                 std::str::from_utf8(&tuple.data[1]).unwrap(),
    //                 tuple.data[2]
    //             );
    //         }
    //     }
    // }

    // #[test]
    // fn header_test() {
    //     let data = include_bytes!("../assets/copy-out-data");
    //     let mut part = Vec::<u8>::new();
    //     let mut i = 0;
    //     loop {
    //         part.extend_from_slice(&data[i * 2..(i + 1) * 2]);

    //         let result = parse_header().parse(part.as_slice());
    //         match result {
    //             Ok(result) => {
    //                 println!("Result: {:?}", result);
    //                 break;
    //                 // for tuple in tuples {
    //                 //     println!(
    //                 //         "{} | {:?}",
    //                 //         //u32::from_be_bytes(tuple.data[0].try_into()),
    //                 //         std::str::from_utf8(&tuple.data[1]).unwrap(),
    //                 //         tuple.data[2]
    //                 //     );
    //                 // }
    //             }
    //             Err(e) => match e {
    //                 UnexpectedParse::Eoi => {
    //                     println!("Incomplete");
    //                     i += 1;
    //                 }
    //                 UnexpectedParse::Unexpected => {
    //                     println!("ERROR: {:?}: {:?}", e, part);
    //                     break;
    //                 }
    //             },
    //         }
    //     }
    // }
}
