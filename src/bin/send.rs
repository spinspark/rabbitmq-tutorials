use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    channel
        .queue_declare(
            "hello".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await?;

    let payload = b"Hello World";

    channel
        .basic_publish(
            "".into(),
            "hello".into(),
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?;

    println!("[x] Sent message");

    connection.close(0, "".into()).await?;

    Ok(())
}
