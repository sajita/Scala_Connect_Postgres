import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.sun.jndi.cosnaming.IiopUrl.Address

class ScalaConnPg {
  val user = "scala"
  val pass = "scala"
  val url = "jdbc:postgresql://localhost:5432/testdb"

  def createConnection(): Any = {
    try {
      val connection: Connection = DriverManager.getConnection(url, user, pass);
      connection
    }
    catch {
      case _: Throwable => println("Could not connect to database")
    }
  }

  def create(name: String, address: String, age: Int): Unit = {
    // type cast Any or Unit type to Connection
    val connection: Connection = this.createConnection().asInstanceOf[Connection];

    try {
      val query = "INSERT INTO employee (name, address, age) VALUES (?,?,?)"
      val preparedStmt: PreparedStatement = connection.prepareStatement(query)
      preparedStmt.setString(1, name)
      preparedStmt.setString(2, address)
      preparedStmt.setInt(3, age)
      preparedStmt.execute()
    } finally {
      connection.close()
    }
  }

  def retrieve() = {
    val connection: Connection = this.createConnection().asInstanceOf[Connection];

    try {
      val statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val results = statement.executeQuery("SELECT * from employee;")

      while (results.next) {
        // put your column name from database
        println(results.getString("name") + results.getString("address") + results.getInt("age"))
      }
    } finally {
      connection.close()
    }
  }

  def update(name: String, address: String): Unit = {
    val connection: Connection = this.createConnection().asInstanceOf[Connection];
    try {
      val query = "UPDATE employee SET address=? WHERE name=?"
      val preparedStmt: PreparedStatement = connection.prepareStatement(query)
      preparedStmt.setString(1, address)
      preparedStmt.setString(2, name)
      preparedStmt.execute()
    } finally {
      connection.close()
    }
  }

  def delete(name: String, address: String): Unit = {
    val connection: Connection = this.createConnection().asInstanceOf[Connection];
    try {
      val query = "DELETE FROM employee WHERE name=? and address=?"
      val preparedStmt: PreparedStatement = connection.prepareStatement(query)
      preparedStmt.setString(1, name)
      preparedStmt.setString(2, address)
      preparedStmt.execute()
    } finally {
      connection.close()
    }
  }
}

object TestAppMain {
  def main(args: Array[String]) {
    val DBconnObject = new ScalaConnPg

    // Insert records
    DBconnObject.create("Jane", "Chabahil", 10)
    DBconnObject.create("Will", "kapan", 40)

    println("Before")
    DBconnObject.retrieve()

    DBconnObject.create("Rick", "gaurighat", 50)
    DBconnObject.delete( name= "Will", "kapan")
    DBconnObject.update("Jane", "kathmandu")

    println("After")
    DBconnObject.retrieve()
  }
}