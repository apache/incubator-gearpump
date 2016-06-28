### Gearpump Redis

***

[Redis](http://redis.io/) is an in-memory data structure store, used as database, cache and message broker. Redis is the most popular key-value database.

Usage :

```
val source = DataSourceProcessor(new RedisSource(channel = "channel.in"), 1)
val sink = DataSinkProcessor(new RedisSink(channel = "channel.out"), 1)
```

`RedisSource` used as a **DataSource** reading from Redis channel .`RedisSink` is a **DataSink** writing message to Redis channel .

```
val sink = DataSinkProcessor(new RedisStorage(), 1)
```

`RedisStorage` is a **DataSink** write message as a key-value pair in Redis . **Processor** could send Message to RedisStorage running the command :

1. `PublishMessage` : Send message to a Redis Channel .

2. `SetMessage` : Set the value for a key .

3. `LPushMessage` : Set the value at the head of a list .

4. `RPushMessage` : Set the value at the tail of a list .

5. `HSetMessage` : Set the value as a field of a hash . 

6. `SAddMessage` : Set the value to a set .

7. `ZAddMessage` : Set the value to a sorted set .

8. `GEOAdd` : Set the value to the HyperLogLog data structure .

