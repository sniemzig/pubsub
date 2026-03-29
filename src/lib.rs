use ahash::{AHashMap, AHasher, RandomState};
use indexmap::{IndexMap, IndexSet};
use std::{
    any::TypeId,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::mpsc;

pub trait Topic<E>: Hash + 'static {
    type Payload;
    fn into_event(self, payload: Self::Payload) -> E;
}

#[derive(Clone)]
pub struct PubSub<E> {
    inner: Arc<PubSubInner<E>>,
}

impl<E: Send + Sync + 'static> PubSub<E> {
    pub fn spawn() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(bus_handler(rx));
        Self {
            inner: Arc::new(PubSubInner {
                cmd_tx: tx,
                next_conn_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn connect(&self) -> Connection<E> {
        let conn_id = self.inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let _ = self
            .inner
            .cmd_tx
            .send(PubSubCmd::Connect { conn_id, event_tx });
        Connection {
            conn_id,
            cmd_tx: self.inner.cmd_tx.clone(),
            event_rx,
        }
    }

    pub fn emit<T: Topic<E>>(&self, topic: T, payload: T::Payload) {
        let _ = self.inner.cmd_tx.send(PubSubCmd::Emit {
            topic_key: hash_topic(&topic),
            event: topic.into_event(payload),
        });
    }
}

pub struct Connection<E> {
    conn_id: ConnId,
    cmd_tx: mpsc::UnboundedSender<PubSubCmd<E>>,
    event_rx: mpsc::UnboundedReceiver<Arc<E>>,
}

impl<E> Connection<E> {
    pub fn subscribe(&self, topic: &impl Topic<E>) {
        let _ = self.cmd_tx.send(PubSubCmd::Subscribe {
            conn_id: self.conn_id,
            topic_key: hash_topic(topic),
        });
    }

    pub fn unsubscribe(&self, topic: &impl Topic<E>) {
        let _ = self.cmd_tx.send(PubSubCmd::Unsubscribe {
            conn_id: self.conn_id,
            topic_key: hash_topic(topic),
        });
    }

    /// This method is cancel safe and will return `None` if the connection is dropped.
    pub async fn recv(&mut self) -> Option<Arc<E>> {
        self.event_rx.recv().await
    }
}

impl<E> Drop for Connection<E> {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PubSubCmd::Disconnect {
            conn_id: self.conn_id,
        });
    }
}

type ConnId = u64;
type TopicKey = u64;

enum PubSubCmd<E> {
    Shutdown,
    Connect {
        conn_id: ConnId,
        event_tx: mpsc::UnboundedSender<Arc<E>>,
    },
    Disconnect {
        conn_id: ConnId,
    },
    Subscribe {
        conn_id: ConnId,
        topic_key: TopicKey,
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

struct PubSubInner<E> {
    cmd_tx: mpsc::UnboundedSender<PubSubCmd<E>>,
    next_conn_id: AtomicU64,
}

impl<E> Drop for PubSubInner<E> {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PubSubCmd::Shutdown);
    }
}

fn hash_topic<T: Topic<E>, E>(topic: &T) -> TopicKey {
    let mut h = AHasher::default();
    TypeId::of::<T>().hash(&mut h);
    topic.hash(&mut h);
    h.finish()
}

async fn bus_handler<E>(mut cmd_rx: mpsc::UnboundedReceiver<PubSubCmd<E>>) {
    let mut conn_txs = AHashMap::<ConnId, mpsc::UnboundedSender<Arc<E>>>::new();
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
            PubSubCmd::Shutdown => break,
            PubSubCmd::Connect { conn_id, event_tx } => {
                conn_txs.insert(conn_id, event_tx);
            }
            PubSubCmd::Disconnect { conn_id } => {
                if let Some(keys) = conn_keys.remove(&conn_id) {
                    for key in keys {
                        remove_sub(&mut index, key, conn_id);
                    }
                }
                conn_txs.remove(&conn_id);
            }
            PubSubCmd::Subscribe { conn_id, topic_key } => {
                if let Some(tx) = conn_txs.get(&conn_id) {
                    index
                        .entry(topic_key)
                        .or_default()
                        .entry(conn_id)
                        .or_insert_with(|| tx.clone());
                    conn_keys.entry(conn_id).or_default().insert(topic_key);
                }
            }
            PubSubCmd::Unsubscribe { conn_id, topic_key } => {
                remove_sub(&mut index, topic_key, conn_id);
                if let Some(keys) = conn_keys.get_mut(&conn_id) {
                    keys.swap_remove(&topic_key);
                }
            }
            PubSubCmd::Emit { topic_key, event } => {
                if let Some(subs) = index.get(&topic_key) {
                    let event = Arc::new(event);
                    for tx in subs.values() {
                        let _ = tx.send(Arc::clone(&event));
                    }
                }
            }
        }
    }
}
