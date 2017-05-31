# Gearpump RabbitMQ

Gearpump integration for [RabbitMQ](https://www.rabbitmq.com/)

## Usage

The message type that RMQSink is able to handle including:

 1. String
 2. Array[Byte]
 3. Sequence of type 1 and 2

Suppose there is a DataSource Task will output above-mentioned messages, you can write a simple application then:

```scala
val sink = new RMQSink(UserConfig.empty, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[DataSource]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("RabbitMQ", Graph(computation), UserConfig.empty)
```