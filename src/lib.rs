use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use ahash::{AHashMap, AHasher, RandomState};
use indexmap::{IndexMap, IndexSet};
use tokio::sync::mpsc;

type ConnId = u64;
type TopicKey = u64;

enum EventBusCmd<E> {
    Shutdown,
    Disconnect {
        conn_id: ConnId,
    },
    Subscribe {
        conn_id: ConnId,
        topic_key: TopicKey,
        event_tx: mpsc::UnboundedSender<Arc<E>>,
    },
    Unsubscribe {
        conn_id: ConnId,
        topic_key: TopicKey,
    },
    Emit {
        topic_key: TopicKey,
        event: E,
    },
}

struct EventBusInner<E> {
    cmd_tx: mpsc::UnboundedSender<EventBusCmd<E>>,
    next_conn_id: AtomicU64,
}

impl<E> Drop for EventBusInner<E> {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(EventBusCmd::Shutdown);
    }
}

#[derive(Clone)]
pub struct EventBus<E> {
    inner: Arc<EventBusInner<E>>,
}

pub trait Topic<E>: Hash {
    type Payload;
    fn into_event(self, payload: Self::Payload) -> E;
}

impl<E: Send + Sync + 'static> EventBus<E> {
    pub fn spawn() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(bus_handler(rx));
        Self {
            inner: Arc::new(EventBusInner {
                cmd_tx: tx,
                next_conn_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn connect(&self) -> Connection<E> {
        let conn_id = self.inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Connection {
            conn_id,
            cmd_tx: self.inner.cmd_tx.clone(),
            event_tx,
            event_rx,
        }
    }

    pub fn emit<T: Topic<E>>(&self, topic: T, payload: T::Payload) {
        let _ = self.inner.cmd_tx.send(EventBusCmd::Emit {
            topic_key: hash_topic(&topic),
            event: topic.into_event(payload),
        });
    }
}

async fn bus_handler<E>(mut cmd_rx: mpsc::UnboundedReceiver<EventBusCmd<E>>) {
    let mut index =
        AHashMap::<TopicKey, IndexMap<ConnId, mpsc::UnboundedSender<Arc<E>>, RandomState>>::new();
    let mut conn_keys = AHashMap::<ConnId, IndexSet<TopicKey, RandomState>>::new();

    let remove_sub = |index: &mut AHashMap<
        TopicKey,
        IndexMap<ConnId, mpsc::UnboundedSender<Arc<E>>, RandomState>,
    >,
                      topic_key: TopicKey,
                      conn_id: ConnId| {
        if let Some(subs) = index.get_mut(&topic_key) {
            subs.swap_remove(&conn_id);
            if subs.is_empty() {
                index.remove(&topic_key);
            }
        }
    };

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            EventBusCmd::Shutdown => break,
            EventBusCmd::Disconnect { conn_id } => {
                if let Some(keys) = conn_keys.remove(&conn_id) {
                    for key in keys {
                        remove_sub(&mut index, key, conn_id);
                    }
                }
            }
            EventBusCmd::Subscribe {
                conn_id,
                topic_key,
                event_tx,
            } => {
                index
                    .entry(topic_key)
                    .or_default()
                    .entry(conn_id)
                    .or_insert(event_tx);
                conn_keys.entry(conn_id).or_default().insert(topic_key);
            }
            EventBusCmd::Unsubscribe { conn_id, topic_key } => {
                remove_sub(&mut index, topic_key, conn_id);
            }
            EventBusCmd::Emit { topic_key, event } => {
                if let Some(subs) = index.get(&topic_key) {
                    let event = Arc::new(event);
                    for tx in subs.values() {
                        let _ = tx.send(Arc::clone(&event));
                    }
                }
            }
        }
    }

    // dropping index closes all event_tx senders,
    // causing every pending conn.recv() to return None
}

pub struct Connection<E> {
    conn_id: ConnId,
    cmd_tx: mpsc::UnboundedSender<EventBusCmd<E>>,
    event_tx: mpsc::UnboundedSender<Arc<E>>,
    event_rx: mpsc::UnboundedReceiver<Arc<E>>,
}

impl<E> Drop for Connection<E> {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(EventBusCmd::Disconnect {
            conn_id: self.conn_id,
        });
    }
}

fn hash_topic(topic: &impl Hash) -> TopicKey {
    let mut h = AHasher::default();
    topic.hash(&mut h);
    h.finish()
}

impl<E> Connection<E> {
    pub fn subscribe(&self, topic: &impl Topic<E>) {
        let _ = self.cmd_tx.send(EventBusCmd::Subscribe {
            conn_id: self.conn_id,
            event_tx: self.event_tx.clone(),
            topic_key: hash_topic(topic),
        });
    }

    pub fn unsubscribe(&self, topic: &impl Topic<E>) {
        let _ = self.cmd_tx.send(EventBusCmd::Unsubscribe {
            conn_id: self.conn_id,
            topic_key: hash_topic(topic),
        });
    }

    pub async fn recv(&mut self) -> Option<Arc<E>> {
        self.event_rx.recv().await
    }
}
