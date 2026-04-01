#[derive(Hash)]
struct ExternalTopic {
    room: u64,
}
impl pubsub::Topic for ExternalTopic {
    type Payload = String;
}

pubsub::event!(SystemTick => u64);

pubsub::events! {
    AppTopics -> AppEvents,
    {
        UserJoined => String,
        LobbyMessage { lobby: u64 } => String,
        ExternalTopic,
        SystemTick,
    }
}

#[tokio::test]
async fn macros_generate_topics_and_events() {
    let bus = pubsub::PubSub::<AppEvents>::spawn();
    let mut conn = bus.connect();

    conn.subscribe(&UserJoined);
    conn.subscribe(&LobbyMessage { lobby: 7 });
    conn.subscribe(&ExternalTopic { room: 99 });
    conn.subscribe(&SystemTick);

    bus.emit(UserJoined, "Alice".to_string());
    bus.emit(LobbyMessage { lobby: 7 }, "hello lobby".to_string());
    bus.emit(ExternalTopic { room: 99 }, "room joined".to_string());
    bus.emit(SystemTick, 42);

    match conn.recv().await.as_deref() {
        Some(AppEvents::UserJoined(event)) => {
            assert_eq!(event.payload, "Alice");
        }
        other => panic!("unexpected event: {}", event_name(other)),
    }

    match conn.recv().await.as_deref() {
        Some(AppEvents::LobbyMessage(event)) => {
            assert_eq!(event.topic.lobby, 7);
            assert_eq!(event.payload, "hello lobby");
        }
        other => panic!("unexpected event: {}", event_name(other)),
    }

    match conn.recv().await.as_deref() {
        Some(AppEvents::ExternalTopic(event)) => {
            assert_eq!(event.topic.room, 99);
            assert_eq!(event.payload, "room joined");
        }
        other => panic!("unexpected event: {}", event_name(other)),
    }

    match conn.recv().await.as_deref() {
        Some(AppEvents::SystemTick(event)) => {
            assert_eq!(event.payload, 42);
        }
        other => panic!("unexpected event: {}", event_name(other)),
    }
}

#[test]
fn macros_generate_topics_enum() {
    let inline_topic = AppTopics::LobbyMessage(LobbyMessage { lobby: 11 });
    let existing_topic = AppTopics::ExternalTopic(ExternalTopic { room: 3 });

    match inline_topic {
        AppTopics::LobbyMessage(topic) => assert_eq!(topic.lobby, 11),
        _ => panic!("unexpected inline topic variant"),
    }

    match existing_topic {
        AppTopics::ExternalTopic(topic) => assert_eq!(topic.room, 3),
        _ => panic!("unexpected existing topic variant"),
    }
}

fn event_name(event: Option<&AppEvents>) -> &'static str {
    match event {
        Some(AppEvents::UserJoined(_)) => "UserJoined",
        Some(AppEvents::LobbyMessage(_)) => "LobbyMessage",
        Some(AppEvents::ExternalTopic(_)) => "ExternalTopic",
        Some(AppEvents::SystemTick(_)) => "SystemTick",
        None => "None",
    }
}
