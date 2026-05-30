use std::env::var;
use std::sync::{Arc, atomic::AtomicU64};

pub struct Bot {
    pub database: Arc<tokio_postgres::Client>,
    pub sequence: Arc<AtomicU64>,
}

impl Bot {
    pub async fn new() -> Self {
        use tokio_postgres::{connect, NoTls};

        let db_conf = format!(
            "host={host} port={port} dbname={dbname} user={user} password={pass}",

            host   = var("DB_SERVER_IPV4").expect("Env var 'DB_SERVER_IPV4' should be set"),
            port   = var("DB_SERVER_PORT").expect("Env var 'DB_SERVER_PORT' should be set"),
            dbname = var("DB_NAME").expect("Env var 'DB_NAME' should be set"),
            user   = var("DB_USER").expect("Env var 'DB_USER' should be set"),
            pass   = var("DB_PASSWORD").expect("Env var 'DB_PASSWORD' should be set"),
        );

        let (client, connection) = connect(db_conf.as_str(), NoTls)
            .await
            .expect("Should have connected to database");

        tokio::spawn(async move {
            connection.await
                .expect("Should have sent database connection to async block");
        });

        println!("Connection to database successful");
        return Bot {
            database: Arc::new(client),
            sequence: Arc::new(AtomicU64::new(0)),
        };
    }
}
