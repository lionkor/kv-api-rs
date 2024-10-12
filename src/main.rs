use kv::entry::Entry;
use tokio::fs::File;

mod kv;

use actix_web::{
    http::header::ACCEPT, web, App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder,
};
use std::sync::Mutex;

struct AppState {
    store: Mutex<kv::store::FileBackedKVStore>,
}

fn accept_header_matches(header: &str, mime_type: &str) -> bool {
    if header.contains(mime_type) || header.contains("*/*") {
        return true;
    }
    if mime_type.ends_with("/*") {
        let base_type = mime_type.trim_end_matches("/*");
        return header.starts_with(base_type) || header.starts_with(&format!("{}/", base_type));
    }
    if header.ends_with("/*") {
        let base_type = header.trim_end_matches("/*");
        return mime_type.starts_with(base_type) || mime_type.starts_with(&format!("{}/", base_type));
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accept_header_matches() {
        assert!(accept_header_matches("text/plain", "text/plain"));
        assert!(accept_header_matches("text/*", "text/plain"));
        assert!(accept_header_matches("*/*", "text/plain"));
        assert!(accept_header_matches("application/json", "application/*"));
        assert!(!accept_header_matches("application/json", "text/plain"));
        assert!(!accept_header_matches("text/html", "application/json"));
        assert!(!accept_header_matches("text/*", "application/json"));
        assert!(!accept_header_matches("text/html", "application/html"));
        assert!(!accept_header_matches("text/html", "application/*"));
    }
}

async fn get_value(
    req: HttpRequest,
    data: web::Data<AppState>,
    key: web::Path<String>,
) -> impl Responder {
    let store = data.store.lock().unwrap();
    match store.get(&key) {
        Some(value) => {
            if let Some(accept_header) = req.headers().get(ACCEPT) {
                if let Ok(accept) = accept_header.to_str() {
                    if !accept_header_matches(accept, &value.mime) {
                        return HttpResponse::NotAcceptable().body("Mismatched MIME type");
                    }
                }
            }
            HttpResponse::Ok()
                .content_type(value.mime.clone())
                .body(value.value.clone())
        }
        None => HttpResponse::NotFound().body("Not Found"),
    }
}

async fn set_value(
    req: HttpRequest,
    data: web::Data<AppState>,
    key: web::Path<String>,
    value: web::Bytes,
) -> impl Responder {
    let mut store = data.store.lock().unwrap();
    match store
        .set(
            &key,
            Entry::new(value.to_vec(), req.content_type().to_string()),
        )
        .await
    {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error setting value: {:?}", e);
            return HttpResponse::InternalServerError().body("Error setting value");
        }
    }
    HttpResponse::Ok().body("OK")
}

async fn start_server(store: kv::store::FileBackedKVStore) -> std::io::Result<()> {
    let data = web::Data::new(AppState {
        store: Mutex::new(store),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/{key}", web::get().to(get_value))
            .route("/{key}", web::post().to(set_value))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

#[actix_web::main]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let mut options = File::options();
    options.write(true);
    options.read(true);
    options.create(true);
    let file = options.open("./test.db").await.unwrap();
    let mut store = kv::store::FileBackedKVStore::new(Box::new(file))
        .await
        .expect("file backed kv store couldnt be created");
    start_server(store).await.unwrap();
}
