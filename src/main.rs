
use std::env;

use lambda::{handler_fn, Context};
use anyhow::{anyhow, Result};
use serde_derive::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use log::{LevelFilter, error};
use rusoto_s3::{
    S3,
    S3Client,
    PutObjectRequest,
};
use rusoto_core::Region;
use rusoto_mock::{
    MockCredentialsProvider,
    MockRequestDispatcher,
    MockResponseReader,
    ReadMockResponse,
};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CustomEvent {
    text_body: Option<String>,
}

#[derive(Serialize, Debug, PartialEq)]
struct CustomOutput {
    message: String,
}

const MOCK_KEY: &str = "AWS_MOCK_FLAG";
const BUCKET_NAME_KEY: &str = "BUCKET_NAME";
const LOCAL_KEY: &str = "LOCAL_FLAG";
const MSG_EMPTY_TEXT_BODY: &str = "Empty text body.";
const MSG_TEXT_BODY_TOO_LONG: &str = "Text body is too long (max: 100)";

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().with_level(LevelFilter::Debug).init().unwrap();
    lambda::run(handler_fn(hello))
        .await
        // https://github.com/dtolnay/anyhow/issues/35
        .map_err(|err| anyhow!(err))?;
    Ok(())
}

async fn hello(event: CustomEvent, c: Context) -> Result<CustomOutput> {
    if let None = event.text_body {
        error!("Empty text body in request {}", c.request_id);
        return Err(anyhow!(get_err_msg(400, MSG_EMPTY_TEXT_BODY)));
    }
    let text = event.text_body.unwrap();
    if text.len() > 100 {
        error!("text body is too long (max: 100) in request {}", c.request_id);
        return Err(anyhow!(get_err_msg(400, MSG_TEXT_BODY_TOO_LONG)));
    }
    let s3 = get_s3_client();
    let bucket_name = env::var(BUCKET_NAME_KEY)?;
    s3.put_object(PutObjectRequest {
        bucket: bucket_name.to_string(),
        key: "test.txt".to_string(),
        body: Some(text.into_bytes().into()),
        acl: Some("public-read".to_string()),
        ..Default::default()
    }).await?;
    
    Ok(CustomOutput {
        message: format!("Succeeded.")
    })
}

fn get_s3_client() -> S3Client {
    let s3 = match env::var(MOCK_KEY) {
        Ok(_) => {
            // Unit Test
            S3Client::new_with(
                MockRequestDispatcher::default().with_body(
                    &MockResponseReader::read_response("mock_data", "s3_test.json")
                ),
                MockCredentialsProvider,
                Default::default(),
            )
        },
        Err(_) => {
            if env::var(LOCAL_KEY).unwrap() != "" {
                // local
                return S3Client::new(Region::Custom {
                    name: "ap-northeast-1".to_owned(),
                    endpoint: "http://host.docker.internal:8000".to_owned(),
                })
            }
            // cloud
            return S3Client::new(Region::ApNortheast1)
        },
    };
    s3
}

fn get_err_msg(code: u16, msg: &str) -> String {
    format!("[{}] {}", code, msg)
}

fn hoge_function() -> String {
    println!("hoge function executed.");
    String::from("hogehoge!")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        env::set_var(MOCK_KEY, "1");
        env::set_var(BUCKET_NAME_KEY, "test-bucket");
    }

    #[test]
    fn can_get_local_s3_client() {
        env::set_var(LOCAL_KEY, "local");
        let _s3 = get_s3_client();
        assert!(true);
    }

    #[test]
    fn can_get_cloud_s3_client() {
        env::set_var(LOCAL_KEY, "");
        let _s3 = get_s3_client();
        assert!(true);
    }

    #[tokio::test]
    async fn can_hello_handler_handle_valid_request() {
        setup();
        let event = CustomEvent {
            text_body: Some("Firstname".to_string())
        };
        let expected = CustomOutput {
            message: "Succeeded.".to_string()
        };
        assert_eq!(
            hello(event, Context::default())
                .await
                .expect("expected Ok(_) value"),
            expected
        )
    }

    #[tokio::test]
    async fn can_hello_handler_handle_empty_text_body() {
        setup();
        let event = CustomEvent {
            text_body: None
        };
        let result = hello(event, Context::default()).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert_eq!(
                error.to_string(),
                format!("[400] {}", MSG_EMPTY_TEXT_BODY)
            )
        } else {
            // result must be Err
            panic!()
        }
    }

    #[tokio::test]
    async fn can_hello_handler_handle_text_body_too_long() {
        setup();
        let event = CustomEvent {
            text_body: Some("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901".to_owned())
        };
        let result = hello(event, Context::default()).await;
        assert!(result.is_err());
        if let Err(error) = result {
            assert_eq!(
                error.to_string(),
                format!("[400] {}", MSG_TEXT_BODY_TOO_LONG)
            )
        } else {
            // result must be Err
            panic!()
        }
    }

    #[test]
    fn can_hoge_function_return_correct_string() {
        let result = hoge_function();
        let expected = String::from("hogehoge!");
        assert_eq!(result, expected);
    }
}
