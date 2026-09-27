use futures_util::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
enum Error {
    CannotDecodeArg,
    MissingReplyTo,
    MissingCorrelationId,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotDecodeArg => write!(f, "Cannot decode argument"),
            Self::MissingReplyTo => write!(f, "Missing 'reply to' property"),
            Self::MissingCorrelationId => write!(f, "Missing 'correlation id' property"),
        }
    }
}

fn fib(n: u64) -> u64 {
    if n < 2 { n } else { fib(n - 1) + fib(n - 2) }
}

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
            "rpc_queue".into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
        )
        .await?;

    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            "rpc_queue".into(),
            "rpc_server".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("[x] Awaiting RPC requests");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            println!("[x] Received {:?}", std::str::from_utf8(&delivery.data)?);
            let n = u64::from_le_bytes(
                delivery
                    .data
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::CannotDecodeArg)?,
            );
            println!("[.] fib({n})");
            let response = fib(n);
            let payload = response.to_be_bytes();

            let routing_key = delivery
                .properties
                .reply_to()
                .clone()
                .ok_or(Error::MissingReplyTo)?;

            let correlation_id = delivery
                .properties
                .correlation_id()
                .clone()
                .ok_or(Error::MissingCorrelationId)?;

            channel
                .basic_publish(
                    "".into(),
                    routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default().with_correlation_id(correlation_id),
                )
                .await?;

            channel
                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                .await?;
        }
    }

    Ok(())
}
