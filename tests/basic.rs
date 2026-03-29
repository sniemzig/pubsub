use std::hash::Hash;

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

impl pubsub::Topic<AppEvent> for LobbyMsg {
    type Payload = String;
    fn into_event(self, text: String) -> AppEvent {
        AppEvent::LobbyMsg {
            lobby: self.lobby,
            text,
        }
    }
}

impl pubsub::Topic<AppEvent> for UserJoined {
    type Payload = String;
    fn into_event(self, name: String) -> AppEvent {
        AppEvent::UserJoined { name }
    }
}

impl pubsub::Topic<AppEvent> for MessageSent {
    type Payload = (String, String);
    fn into_event(self, (from, text): (String, String)) -> AppEvent {
        AppEvent::MessageSent { from, text }
    }
}

#[tokio::test]
async fn pubsub_basic_works() {
    let bus = pubsub::PubSub::<AppEvent>::spawn();
    let mut conn: pubsub::Connection<AppEvent> = bus.connect();

    conn.subscribe(&UserJoined);
    conn.subscribe(&MessageSent);
    conn.subscribe(&LobbyMsg { lobby: 64 });

    bus.emit(UserJoined, "Alice".to_string());
    bus.emit(
        MessageSent,
        ("Alice".to_string(), "Hello, world!".to_string()),
    );
    bus.emit(LobbyMsg { lobby: 64 }, "Welcome to the lobby!".to_string());
    bus.emit(LobbyMsg { lobby: 65 }, "Welcome to the lobby!".to_string()); // should be ignored
    bus.emit(
        MessageSent,
        ("Alice".to_string(), "Hello, world!".to_string()),
    );

    let expected_events = [
        AppEvent::UserJoined {
            name: "Alice".to_string(),
        },
        AppEvent::MessageSent {
            from: "Alice".to_string(),
            text: "Hello, world!".to_string(),
        },
        AppEvent::LobbyMsg {
            lobby: 64,
            text: "Welcome to the lobby!".to_string(),
        },
        // LobbyMsg for lobby 65 should not be received since we didn't subscribe to it
        AppEvent::MessageSent {
            from: "Alice".to_string(),
            text: "Hello, world!".to_string(),
        },
    ];

    for expected in &expected_events {
        assert_eq!(conn.recv().await.as_deref(), Some(expected));
    }

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
