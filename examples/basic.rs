use std::hash::Hash;

use event_bus::{EventBus, Topic};

// A single enum for all events on this bus.
#[derive(Debug)]
enum AppEvent {
    UserJoined { name: String },
    MessageSent { from: String, text: String },
}

// Each topic is a zero-sized type used both as the hash key
// and as the constructor for its specific event variant.

#[derive(Hash)]
struct UserJoined;

#[derive(Hash)]
struct MessageSent;

impl Topic<AppEvent> for UserJoined {
    type Payload = String;
    fn into_event(self, name: String) -> AppEvent {
        AppEvent::UserJoined { name }
    }
}

impl Topic<AppEvent> for MessageSent {
    type Payload = (String, String);
    fn into_event(self, (from, text): (String, String)) -> AppEvent {
        AppEvent::MessageSent { from, text }
    }
}

#[tokio::main]
async fn main() {
    let bus = EventBus::<AppEvent>::spawn();

    let mut conn = bus.connect();

    // subscribe before emitting so the bus handler processes
    // the subscriptions first (same channel, guaranteed FIFO ordering)
    conn.subscribe(&UserJoined);
    conn.subscribe(&MessageSent);

    bus.emit(UserJoined, "Alice".to_string());
    bus.emit(
        MessageSent,
        ("Alice".to_string(), "Hello, world!".to_string()),
    );

    // recv() returns Option<Arc<AppEvent>>
    for _ in 0..2 {
        if let Some(event) = conn.recv().await {
            match event.as_ref() {
                AppEvent::UserJoined { name } => println!("{name} joined"),
                AppEvent::MessageSent { from, text } => println!("[{from}]: {text}"),
            }
        }
    }

    // conn is dropped here, which automatically sends Disconnect to the bus
}
