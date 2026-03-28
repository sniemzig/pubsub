use std::hash::Hash;

use event_bus::{EventBus, Topic};

#[derive(Debug, PartialEq)]
enum AppEvent {
    UserJoined { name: String },
    MessageSent { from: String, text: String },
    LobbyMsg { lobby: u64, text: String },
}

#[derive(Hash)]
struct UserJoined;

#[derive(Hash)]
struct MessageSent;

#[derive(Hash)]
struct LobbyMsg {
    lobby: u64,
}

impl Topic<AppEvent> for LobbyMsg {
    type Payload = String;
    fn into_event(self, text: String) -> AppEvent {
        AppEvent::LobbyMsg {
            lobby: self.lobby,
            text,
        }
    }
}

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

    conn.subscribe(&UserJoined);
    conn.subscribe(&MessageSent);
    conn.subscribe(&LobbyMsg { lobby: 64 });

    bus.emit(UserJoined, "Alice".to_string());
    bus.emit(
        MessageSent,
        ("Alice".to_string(), "Hello, world!".to_string()),
    );
    bus.emit(LobbyMsg { lobby: 64 }, "Welcome to the lobby!".to_string());
    bus.emit(LobbyMsg { lobby: 65 }, "Welcome to the lobby!".to_string());
    bus.emit(
        MessageSent,
        ("Alice".to_string(), "Hello, world!".to_string()),
    );

    assert_eq!(
        conn.recv().await.as_deref(),
        Some(&AppEvent::UserJoined {
            name: "Alice".to_string()
        })
    );
    assert_eq!(
        conn.recv().await.as_deref(),
        Some(&AppEvent::MessageSent {
            from: "Alice".to_string(),
            text: "Hello, world!".to_string()
        })
    );
    assert_eq!(
        conn.recv().await.as_deref(),
        Some(&AppEvent::LobbyMsg {
            lobby: 64,
            text: "Welcome to the lobby!".to_string()
        })
    );
    assert_eq!(
        conn.recv().await.as_deref(),
        Some(&AppEvent::MessageSent {
            from: "Alice".to_string(),
            text: "Hello, world!".to_string()
        })
    );

    conn.subscribe(&LobbyMsg { lobby: 65 });
    bus.emit(LobbyMsg { lobby: 65 }, "Welcome to the lobby!".to_string());

    assert_eq!(
        conn.recv().await.as_deref(),
        Some(&AppEvent::LobbyMsg {
            lobby: 65,
            text: "Welcome to the lobby!".to_string()
        })
    );
}
