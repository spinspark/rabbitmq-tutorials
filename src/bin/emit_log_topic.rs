use lapin::{
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let routing_key = args.first().map_or("anonymous.info", String::as_str);
    let message = match args.len() {
        x if x < 2 => "Hello, world!".to_string(),
        _ => args[1..].join(" "),
    };

    let addr = "amqp://127.0.0.1:5672";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    channel
        .exchange_declare(
            "topic_logs".into(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .basic_publish(
            "topic_logs".into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            message.as_bytes(),
            BasicProperties::default(),
        )
        .await?;

    println!(
        "[x] Sent {routing_key}:{:?}",
        std::str::from_utf8(message.as_bytes())?
    );

    connection.close(0, "".into()).await?;

    Ok(())
}
