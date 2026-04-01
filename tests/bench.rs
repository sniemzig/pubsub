// Move this into a actual benchmark suite like `criterion`...

use std::time::Instant;
use tokio::time::{Duration, sleep};

#[derive(Debug, PartialEq)]
enum BenchEvent {
    Tick(u64),
}

#[derive(Hash, Clone)]
struct BenchTopic;

impl pubsub::Topic for BenchTopic {
    type Payload = u64;
}

impl pubsub::IntoEvent<BenchEvent> for BenchTopic {
    fn into_event(self, payload: Self::Payload) -> BenchEvent {
        BenchEvent::Tick(payload)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_throughput_benchmark() {
    let bus = pubsub::PubSub::<BenchEvent>::spawn();

    // Tweak these numbers to see how your system scales
    let num_messages: u64 = 1_000_000;
    let num_subscribers: u64 = 10;

    let mut join_handles = vec![];

    // 1. Spawn Subscribers
    for _ in 0..num_subscribers {
        let mut conn = bus.connect();
        conn.subscribe(&BenchTopic);

        let handle = tokio::spawn(async move {
            let mut received = 0;
            while let Some(_) = conn.recv().await {
                received += 1;
                if received == num_messages {
                    break;
                }
            }
        });
        join_handles.push(handle);
    }

    // Give the central bus_handler a tiny moment to process all the Connect
    // and Subscribe commands before we start blasting events.
    sleep(Duration::from_millis(50)).await;

    // 2. Start the Timer & Spawn Publisher
    let start_time = Instant::now();
    let bus_clone = bus.clone();

    let pub_handle = tokio::spawn(async move {
        for i in 0..num_messages {
            // We clone the topic just to satisfy ownership if needed,
            // though unit structs are cheap to recreate.
            bus_clone.emit(BenchTopic, i);
        }
    });

    // 3. Wait for publisher to finish sending
    pub_handle.await.unwrap();

    // 4. Wait for all subscribers to finish receiving
    for handle in join_handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    let emits_per_sec = (num_messages as f64 / elapsed_secs) as u64;
    let routed_per_sec = ((num_messages * num_subscribers) as f64 / elapsed_secs) as u64;

    println!("========================================");
    println!("⏱️  PUBSUB THROUGHPUT RESULTS");
    println!("========================================");
    println!("Messages Emitted : {}", num_messages);
    println!("Active Subscribers: {}", num_subscribers);
    println!("Total Time       : {:?}", elapsed);
    println!("----------------------------------------");
    println!("Emit Throughput  : {} msgs/sec", emits_per_sec);
    println!("Route Throughput : {} msgs/sec (Fan-out)", routed_per_sec);
    println!("========================================");
}
