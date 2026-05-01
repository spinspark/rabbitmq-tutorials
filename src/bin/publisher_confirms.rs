use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use std::time::Instant;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    publish_messages_individually().await?;
    publish_messages_in_batch().await?;
    Ok(())
}

async fn publish_messages_individually() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://localhost";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    let queue = Uuid::new_v4().to_string();
    channel
        .queue_declare(
            queue.clone().into(),
            QueueDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;

    let start = Instant::now();
    for i in 0..50000_i32 {
        channel
            .basic_publish(
                "".into(),
                queue.clone().into(),
                BasicPublishOptions::default(),
                &i.to_be_bytes(),
                BasicProperties::default(),
            )
            .await?;
        channel.wait_for_confirms().await?;
    }

    println!(
        "[x] Published {} messages individually in {}ms",
        50000,
        start.elapsed().as_millis()
    );

    connection.close(0, "".into()).await?;
    Ok(())
}

async fn publish_messages_in_batch() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://localhost";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    let queue = Uuid::new_v4().to_string();
    channel
        .queue_declare(
            queue.clone().into(),
            QueueDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;

    let batch_size = 100;
    let mut outstanding_message_count = 0;

    let start = Instant::now();
    for i in 0..50000_i32 {
        channel
            .basic_publish(
                "".into(),
                queue.clone().into(),
                BasicPublishOptions::default(),
                &i.to_be_bytes(),
                BasicProperties::default(),
            )
            .await?;
        outstanding_message_count += 1;

        if outstanding_message_count == batch_size {
            channel.wait_for_confirms().await?;
            outstanding_message_count = 0;
        }
    }

    if outstanding_message_count > 0 {
        channel.wait_for_confirms().await?;
    }

    println!(
        "[x] Published {} messages in batch in {}ms",
        50000,
        start.elapsed().as_millis()
    );

    connection.close(0, "".into()).await?;
    Ok(())
}
