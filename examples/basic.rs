enum Event {
    Toast { text: String },
    LobbyMsg { lobby: u64, text: String },
}

#[derive(Hash)]
struct ToastTopic;
impl pubsub::Topic<Event> for ToastTopic {
    type Payload = String;
    fn into_event(self, text: String) -> Event {
        Event::Toast { text }
    }
}

#[derive(Hash)]
struct LobbyMsgTopic {
    lobby: u64,
}
impl pubsub::Topic<Event> for LobbyMsgTopic {
    type Payload = String;
    fn into_event(self, text: String) -> Event {
        Event::LobbyMsg {
            lobby: self.lobby,
            text,
        }
    }
}

#[tokio::main]
async fn main() {
    let ps = pubsub::PubSub::<Event>::spawn();
    let mut conn = ps.connect();
    conn.subscribe(&ToastTopic);
    conn.subscribe(&LobbyMsgTopic { lobby: 1234 });

    ps.emit(ToastTopic, "Hello, world!".to_string());
    ps.emit(
        LobbyMsgTopic { lobby: 1234 },
        "Welcome to the lobby!".to_string(),
    );
    ps.emit(
        LobbyMsgTopic { lobby: 5678 },
        "This should not be received since we didn't subscribe to it".to_string(),
    );
    drop(ps);

    while let Some(e) = conn.recv().await {
        match e.as_ref() {
            Event::Toast { text } => println!("Toast: {}", text),
            Event::LobbyMsg { lobby, text } => println!("Lobby {}: {}", lobby, text),
        }
    }
}
