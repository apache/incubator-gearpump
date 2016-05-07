package io.gearpump.streaming.redis

import java.nio.charset.Charset

object RedisMessage {

  private def toBytes(string: String,
                      charset: Charset = Charset.forName("UTF8")
                     ): Array[Byte] = string.getBytes(charset)

  case class PublishMessage(message: Array[Byte]) {
    def this(message: String) = this(toBytes(message))
  }

  case class SetMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  case class LPushMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  case class RPushMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  case class HSetMessage(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
    def this(key: String, field: String, value: String) = this(toBytes(key), toBytes(field), toBytes(value))
  }

  case class SAddMessage(key: Array[Byte], member: Array[Byte]) {
    def this(key: String, member: String) = this(toBytes(key), toBytes(member))
  }

  case class ZAddMessage(key: Array[Byte], score: Double, member: Array[Byte]) {
    def this(key: String, score: Double, member: String) = this(toBytes(key), score, toBytes(member))
  }

  case class PFAdd(key: Array[Byte], member: Array[Byte]) {
    def this(key: String, member: String) = this(toBytes(key), toBytes(member))
  }

  case class GEOAdd(key: Array[Byte], longitude: Double, latitude: Double, member: Array[Byte]) {
    def this(key: String, longitude: Double, latitude: Double, member: String) =
      this(toBytes(key), longitude, latitude, toBytes(member))
  }

}
