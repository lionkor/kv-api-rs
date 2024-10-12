use kv::entry::Entry;
use tokio::fs::File;

mod kv;

use actix_web::{web, App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder};
use std::sync::Mutex;

struct AppState {
    store: Mutex<kv::store::FileBackedKVStore>,
}

async fn get_value(data: web::Data<AppState>, key: web::Path<String>) -> impl Responder {
    let store = data.store.lock().unwrap();
    match store.get(&key) {
        Some(value) => HttpResponse::Ok()
                .content_type(value.mime.clone())
                .body(value.value.clone()),
        None => HttpResponse::NotFound().body("Key not found"),
    }
}

async fn set_value(req: HttpRequest, data: web::Data<AppState>, key: web::Path<String>, value: web::Bytes) -> impl Responder {
    let mut store = data.store.lock().unwrap();
    match store.set(&key, Entry::new(value.to_vec(), req.content_type().to_string())).await {
        Ok(_) => (),
        Err(e) => {
            log::error!("Error setting value: {:?}", e);
            return HttpResponse::InternalServerError().body("Error setting value");
        }
    }
    HttpResponse::Ok().body("Value set")
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
    let mut store =
        kv::store::FileBackedKVStore::new(Box::new(file))
            .await
            .expect("file backed kv store couldnt be created");
    start_server(store).await.unwrap();
}
