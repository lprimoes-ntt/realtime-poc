use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;

const BROKER: &str = "localhost:19092";
const TOPICS: &[&str] = &["shori_data.ShoriDB.dbo.Users"];

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
    println!("Connecting to {BROKER}...");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set("group.id", "shori-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("Failed to create consumer");

    consumer
        .subscribe(TOPICS)
        .expect("Failed to subscribe to topics");

    println!("Subscribed to: {}", TOPICS.join(", "));
    println!("Waiting for CDC events...\n");

    let mut backoff = INITIAL_BACKOFF;

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                backoff = INITIAL_BACKOFF;

                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();

                let payload = msg
                    .payload_view::<str>()
                    .and_then(|r| r.ok())
                    .unwrap_or("<no payload>");

                println!("[{topic}  p:{partition}  o:{offset}]");
                println!("{payload}\n");
            }

            Err(KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)) => {
                eprintln!(
                    "Topic not yet available, retrying in {}s... \
                     (waiting for Debezium to create CDC topics)",
                    backoff.as_secs()
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }

            Err(KafkaError::PartitionEOF(partition)) => {
                eprintln!("Reached end of partition {partition}, waiting for new messages...");
            }

            Err(KafkaError::MessageConsumptionFatal(code)) => {
                eprintln!("Fatal consumer error: {code}");
                std::process::exit(1);
            }

            Err(e) => {
                eprintln!("Consumer error: {e}");
            }
        }
    }
}
