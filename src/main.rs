// File/data handling
use std::env::var as v;
use json::{object, JsonValue, parse as to_json, stringify as jstr};

// Async function stuff
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::sync::Mutex;

// WebSocket
use futures_util::{SinkExt, StreamExt};
use futures_util::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::protocol::Message;

// Postgres
use tokio_postgres::Client;

#[tokio::main]
async fn main() {
    dotenv::dotenv().expect("Should have loaded '.env' file");

    // Initialise database and WebSocket
    println!("Initialising connections...");
    let db = Arc::new(init_db().await);
    let s = Arc::new(AtomicU64::new(0));
    let (wsw, mut wsr) = init_websocket(s.clone()).await;

    // Continue to read Discord event messages
    while let Message::Text(msg) = wsr.next().await.unwrap()
        .expect("should have read WebSocket message") {

        let data = to_json(&msg).expect("Should have parsed WebSocket message to JSON");

        let op = (&data["op"]).as_i8().expect("Could not read 'op' from JSON object");
        match op {
            0 => {
                let d = &data["d"];
                let t = data["t"].as_str()
                    .expect("Should have read 't' from JSON object");
                s.store(
                    data["s"].as_u64().expect("Should have read 's' from JSON object"),
                    Ordering::SeqCst,
                );

                event_handler(d, t, db.clone()).await;
            },

            1 => wsw.lock().await.send(jstr(object!{
                    "op": 1,
                    "d": s.load(Ordering::SeqCst)
                }).into()).await
                    .expect("Should have sent heartbeat after heartbeat request"),

            7 => println!("Discord says to reconnect ASAP!!!"),

            9 => println!("Invalid session: reconnect and identify/resume"),

            11 => (),

            31 => println!("Discord sent soundboard sounds"),

            _ => println!("Unhandled opcode: {}", op),
        }
    }
}

async fn init_db() -> Client {
    use tokio_postgres::{connect, NoTls};

    let db_conf = format!(
        "host={host} port={port} dbname={dbname} user={user} password={pass}",

        host   = v("DB_HOST")        .expect("Env var 'DB_HOST' should be set"),
        port   = v("DB_PORT")        .expect("Env var 'DB_PORT' should be set"),
        dbname = v("DB_NAME")        .expect("Env var 'DB_NAME' should be set"),
        user   = v("DISCORD_DB_USER").expect("Env var 'DISCORD_DB_USER' should be set"),
        pass   = v("DISCORD_DB_PASS").expect("Env var 'DISCORD_DB_PASS' should be set"),
    );

    let (client, connection) = connect(db_conf.as_str(), NoTls).await
        .expect("Should have connected to database");

    tokio::spawn(async move {
        connection.await.expect("Should have sent database connection to async block");
    });

    println!("Connection to database successful");
    return client;
}

async fn init_websocket(s: Arc<AtomicU64>) -> (
    Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>
) {
    use std::io::{stdin, stdout, Write};
    use tokio::time::{interval, Duration};
    use tokio_tungstenite::connect_async;

    // Initialise WebSocket
    let url = v("GATEWAY_URL").expect("Env var 'GATEWAY_URL' should be set in '.env'");
    let (websocket, _) = connect_async(url).await
        .expect("WebSocket should have connected");
    let (wsw, mut wsr) = websocket.split();
    let wsw = Arc::new(Mutex::new(wsw));
    println!("WebSocket initialised");

    // Build identity payload
    let token   = v("GATEWAY_TOKEN")  .expect("env var 'GATEWAY_TOKEN' should be set");
    let os      = v("OS")             .expect("env var 'OS' should be set");
    let browser = v("BROWSER")        .expect("env var 'BROWSER' should be set");
    let device  = v("DEVICE")         .expect("env var 'DEVICE' should be set");
    let intents = v("GATEWAY_INTENTS").expect("env var 'GATEWAY_INTENTS' should be set");

    let identity_payload = object!{
        "op": 2,
        "d": {
            "token": token,
            "properties": {
                "os":      os,
                "browser": browser,
                "device":  device,
            },
            "presence": {
                "status":     "online",
                "afk":        false,
                "activities": [],
            },
            "intents": intents,
        },
    };

    // Manage first payload
    if let Message::Text(msg) = wsr.next().await.unwrap()
    .expect("WebSocket message should be readable") {

        let data = to_json(&msg).expect("Message should be parsed to JSON");

        if data["op"].as_i8() == Some(10) {
            let hwsw = Arc::downgrade(&wsw);
            let hseq = Arc::downgrade(&s);

            // Create heartbeat block
            tokio::spawn(async move {
                let mut timer = interval(Duration::from_millis(
                    data["d"]["heartbeat_interval"]
                        .as_u64()
                        .expect("Should have read 'heartbeat_interval' from JSON object")
                ));

                loop {
                    timer.tick().await;
                    let heart_payload = jstr(object!{
                        "op": 1,
                        "d": hseq.upgrade().unwrap().load(Ordering::SeqCst),
                    });

                    hwsw.upgrade()
                        .expect("Heartbeat's weak pointer to 'wsw' SplitSink should have upgraded")
                        .lock().await.send(heart_payload.into()).await
                        .expect("Should have sent heartbeat to Discord");
                }
            });

            // Send Identify payload
            let Message::Text(msg) = wsr.next().await.unwrap()
                .expect("Should have read WebSocket message")
            else {
                panic!("Message was not Text when preparing to send identity payload");
            };

            let data = to_json(&msg)
                .expect("Should have parsed WebSocket message to JSON");
            let op = data["op"].as_i8()
                .expect("Should have read 'op' from JSON object");

            if op == 11 {
                wsw.lock().await.send(jstr(identity_payload).into()).await
                    .expect("Failed to send Identity payload");

            } else {
                todo!("Reconnect not yet handled");
            }
        }
    }

    let mut input = String::new();
    while input.as_str() != "Y" || input.as_str() != "N" {
        print!("Connect to voice channel? [y/N]: ");
        let _ = stdout().flush();
        let _ = stdin().read_line(&mut input);
        input.retain(|c| c != '\n' && c != '\r');
        if input == "Y" || input == "y" { break; }
        if input.len() == 0 || input == "N" || input == "n" { return (wsw, wsr); }
        input.truncate(0);
    }
    println!("Voice Gateway connection not yet implemented");

    return (wsw, wsr);
}

async fn event_handler(d: &JsonValue, event: &str, db: Arc<Client>) {
    match event {
        "READY" => println!("Connection with Discord established successfully"),

        "GUILD_CREATE" => {if !db.is_closed() {
            db.query("SELECT discord.process_guild($1);", &[&jstr(d.clone())]).await
                .expect("Should have sent guild object to database for processing");
        }},

        "MESSAGE_CREATE" => insert_message(db, d).await,

        "MESSAGE_DELETE" => delete_message(db, d["id"].as_str()
            .expect("Should have read 'id' from JSON object")).await,

        "MESSAGE_DELETE_BULK" => {
            for i in 0..d["ids"].len() {
                let id = d["ids"][i].as_str()
                    .expect("Should have read index {i} from array 'ids' in JSON object");
                delete_message(db.clone(), id).await;
            }
        },

        "TYPING_START" => (),

        _ => eprintln!("Unhandled event: {}", event),
    }
}

async fn insert_message(db: Arc<Client>, data: &JsonValue) {
    if db.is_closed() { return; }

    // Process user first
    let process_user = db.prepare("SELECT discord.process_user($1);").await.unwrap();
    db.execute(&process_user, &[&data["author"].dump().as_str()]).await
        .expect("Should have sent user object to database for processing");

    // Process mentions
    let mut mid_arr = vec![];
    let mut iter = data["mentions"].members();
    while let Some(obj) = iter.next() {mid_arr.push(obj["id"].as_str()
        .expect("Should have read 'id' from array 'mentions' in JSON object"));
    }
    
    // Process mention_channels
    let mut mcid_arr = vec![];
    let mut iter = data["mention_channels"].members();
    while let Some(obj) = iter.next() {mcid_arr.push(obj["id"].as_str()
        .expect("Should have read 'id' from array 'mention_roles' in JSON object"));
    }

    // Build message record
    let msg_rec = object!{
        "id":                  data["id"].as_str(),
        "channel_id":          data["channel_id"].as_str(),
        "user_id":             data["author"]["id"].as_str(),
        "content":             data["content"].as_str(),
        "timestamp":           data["timestamp"].as_str(),
        "edited_timestamp":    data["edited_timestamp"].as_str(),
        "tts":                 data["tts"].as_str(),
        "mention_everyone":    data["mention_everyone"].as_str(),
        "mention_ids":         mid_arr,
        "mention_role_ids":    data["mention_roles"].clone(),
        "mention_channel_ids": mcid_arr,
        "embeds":              data["embeds"].as_str(),
        "reactions":           data["reactions"].as_str(),
        "nonce":               data["nonce"].as_str(),
        "webhook_id":          data["webhook_id"].as_str(),
        "type":                data["type"].as_number(),
        "flags":               data["flags"].as_number(),
        "msg_ref":             data["message_reference"].as_str(),
        "ref_msg_id":          data["referenced_message"]["id"].as_str(),
        "int_data":            data["interaction_metadata"].as_str(),
        "components":          data["components"].as_str(),
        "stickers":            data["stickers"].as_str(),
        "pos":                 data["position"].as_number(),
        "resolved":            data["resolved"].as_str(),
        "poll":                data["poll"].as_str(),
    };

    // Send message record to database
    db.query(
        "INSERT INTO discord.messages
        SELECT * FROM jsonb_populate_record(
            null::discord.messages,
            CAST($1::text AS jsonb)
        );",
        &[&jstr(msg_rec).as_str()]
    ).await.expect("Should have sent message to database for processing");
}

async fn delete_message(db: Arc<Client>, ids: &str) {
    if db.is_closed() { return; }
    db.query(
        "UPDATE discord.messages SET deleted = TRUE
        WHERE id = CAST($1::text AS bigint);",
        &[&ids]
    ).await.expect("Should have marked message in database as deleted");
}
