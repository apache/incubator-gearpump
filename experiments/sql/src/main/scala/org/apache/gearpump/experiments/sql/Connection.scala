package org.apache.gearpump.experiments.sql

import java.lang.reflect.Type
import java.sql.{Blob, CallableStatement, Clob, DatabaseMetaData, NClob, PreparedStatement, SQLException, SQLWarning, SQLXML, Savepoint, Statement, Struct}
import java.util.Properties
import java.util.concurrent.Executor
import java.{sql, util}

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Enumerator, Queryable}
import org.apache.log4j.Logger

/**
  * Created by Buddhi on 6/8/2017.
  */
class Connection extends CalciteConnection {

  import org.apache.calcite.schema.SchemaPlus
  import org.apache.calcite.tools.Frameworks

  private val logger = Logger.getLogger(classOf[Nothing])
  private val rootSchema = Frameworks.createRootSchema(true)
  private var schema = ""

  @throws[SQLException]
  override def setSchema(s: String): Unit = {
    schema = s
  }

  override def getSchema: String = schema

  override def getTypeFactory: JavaTypeFactory = ???

  override def getProperties: Properties = ???

  override def getRootSchema: SchemaPlus = rootSchema

  override def config(): CalciteConnectionConfig = ???

  override def commit(): Unit = ???

  override def getHoldability: Int = 0

  override def setCatalog(catalog: String): Unit = ???

  override def setHoldability(holdability: Int): Unit = ???

  override def prepareStatement(sql: String): PreparedStatement = ???

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement = ???

  override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement = ???

  override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement = ???

  override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement = ???

  override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement = ???

  override def createClob(): Clob = ???

  override def setClientInfo(name: String, value: String): Unit = ???

  override def setClientInfo(properties: Properties): Unit = ???

  override def createSQLXML(): SQLXML = ???

  override def getCatalog: String = ???

  override def createBlob(): Blob = ???

  override def createStatement(): Statement = ???

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement = ???

  override def createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement = ???

  override def abort(executor: Executor): Unit = ???

  override def setAutoCommit(autoCommit: Boolean): Unit = ???

  override def getMetaData: DatabaseMetaData = ???

  override def setReadOnly(readOnly: Boolean): Unit = ???

  override def prepareCall(sql: String): CallableStatement = ???

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = ???

  override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement = ???

  override def setTransactionIsolation(level: Int): Unit = ???

  override def getWarnings: SQLWarning = ???

  override def releaseSavepoint(savepoint: Savepoint): Unit = ???

  override def nativeSQL(sql: String): String = ???

  override def isReadOnly: Boolean = ???

  override def createArrayOf(typeName: String, elements: Array[AnyRef]): sql.Array = ???

  override def setSavepoint(): Savepoint = ???

  override def setSavepoint(name: String): Savepoint = ???

  override def close(): Unit = ???

  override def createNClob(): NClob = ???

  override def rollback(): Unit = ???

  override def rollback(savepoint: Savepoint): Unit = ???

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = ???

  override def setTypeMap(map: util.Map[String, Class[_]]): Unit = ???

  override def isValid(timeout: Int): Boolean = ???

  override def getAutoCommit: Boolean = ???

  override def clearWarnings(): Unit = ???

  override def getNetworkTimeout: Int = 0

  override def isClosed: Boolean = ???

  override def getTransactionIsolation: Int = 0

  override def createStruct(typeName: String, attributes: Array[AnyRef]): Struct = ???

  override def getClientInfo(name: String): String = ???

  override def getClientInfo: Properties = ???

  override def getTypeMap: util.Map[String, Class[_]] = ???

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???

  override def execute[T](expression: Expression, aClass: Class[T]): T = ???

  override def execute[T](expression: Expression, `type`: Type): T = ???

  override def executeQuery[T](queryable: Queryable[T]): Enumerator[T] = ???

  override def createQuery[T](expression: Expression, aClass: Class[T]): Queryable[T] = ???

  override def createQuery[T](expression: Expression, `type`: Type): Queryable[T] = ???
}
