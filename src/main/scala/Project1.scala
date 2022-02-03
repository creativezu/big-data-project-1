import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1Ex {
  def main(args: Array[String]): Unit = {

    // This block of code is all necessary for spark/hive/hadoop
    System.setSecurityManager(null)

    // change if winutils.exe is in a different bin folder
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Project1Ex") // Change to whatever app name you want
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    //This block to connect to mySQL
    val driver = "com.mysql.cj.jdbc.Driver"
    val url =
      "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
    val username = "root"
    val password = "70Sevenkawkroot!" // Update to include your password
    var connection: Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    // Method to check login credentials
    val adminCheck = login(connection)
    if (adminCheck) {
      print(
        "\n\nWELCOME ADMIN, QUERY THIS DATASET \nBY CHOOSING ONE OF THE FOLLOWING\n..................................\n\n* Top 10 happiest countries        [1]\n* Countries with highest GDP       [2]\n* Countries with the most freedom  [3]\n* Countries with the least crime   [4]\n* Healthy life expectancy          [5]\n* Most generous countries          [6]\n\nEnter your selection here: "
      )

    } else {
      print(
        "\n\n-- WELCOME USER, QUERY THIS DATASET\n BY CHOOSING ONE OF THE FOLLOWING\n\n..................................\n\n* Top 10 happiest countries        [1]\n* Countries with highest GDP       [2]\n* Countries with the most freedom  [3]\n* Countries with the least crime   [4]\n* Healthy life expectancy          [5]\n* Most generous countries          [6]\n\nEnter your selection here: "
      )

    }

    // Run method to insert happy data.
    insertHappyData(hiveCtx)

    /*
     * Here is where I would ask the user for input on what queries they would like to run, as well as
     * method calls to run those queries. An example is below, topGDP(hiveCtx)
     *
     */

    // Which query are we running?
    var s = new Scanner(System.in)
    var query_choice = s.nextInt()
    if (query_choice == 1) {
      topTen(hiveCtx)
    } else if (query_choice == 2) {
      topGDP(hiveCtx)
    } else if (query_choice == 3) {
      topFreedom(hiveCtx)
    } else if (query_choice == 4) {
      leastCrime(hiveCtx)
    } else if (query_choice == 5) {
      lifeExpectancy(hiveCtx)
    } else if (query_choice == 6) {
      mostGenerous(hiveCtx)
    } else if (query_choice == 7) {
      updateCredentials(hiveCtx)
    } else if (query_choice == 8) {
      deleteUser(hiveCtx)
    }

    sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
  }

  // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
  // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
  def login(connection: Connection): Boolean = {

    while (true) {
      val statement = connection.createStatement()
      val statement2 = connection.createStatement()

      println("    __  __                  _                     ")
      println("   / / / /___ _____  ____  (_)___  ___  __________")
      println("  / /_/ / __ `/ __ \\/ __ \\/ / __ \\/ _ \\/ ___/ ___/")
      println(" / __  / /_/ / /_/ / /_/ / / / / /  __(__  |__  ) ")
      println(
        "/_/ /_/\\__,_/ .___/ .___/_/_/ /_/\\___/____/____/   World Happiness Report — Project 1"
      )
      println("           /_/   /_/                              ")

      println("\n\n")
      println(
        "\n\nPLEASE LOGIN HERE\n......................\n"
      )
      print("\nEnter username: ")
      var scanner = new Scanner(System.in)
      var username = scanner.nextLine().trim()

      print("Enter password: ")
      var password = scanner.nextLine().trim()
      val resultSet = statement.executeQuery(
        "SELECT COUNT(*) FROM admin_accounts WHERE username='" + username + "' AND password='" + password + "';"
      )
      while (resultSet.next()) {
        if (resultSet.getString(1) == "1") {
          return true;
        }
      }

      val resultSet2 = statement2.executeQuery(
        "SELECT COUNT(*) FROM user_accounts WHERE username='" + username + "' AND password='" + password + "';"
      )
      while (resultSet2.next()) {
        if (resultSet2.getString(1) == "1") {
          return false;
        }
      }

      println("Username/password combo not found. Try again!")
    }
    return false
  }

  def insertHappyData(hiveCtx: HiveContext): Unit = {

    // This statement creates a DataFrameReader from your file that you wish to pass in.
    val output = hiveCtx.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/happiness_2019.csv")
    //output.limit(15).show()

    // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
    // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this
    // code as well as the creation of output will not be necessary.
    output.createOrReplaceTempView("tmp_data")
    hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    hiveCtx.sql("SET hive.enforce.bucketing=false")
    hiveCtx.sql("SET hive.enforce.sorting=false")
    hiveCtx.sql(
      "CREATE TABLE IF NOT EXISTS happy_2019_1E (overall_rank INT, country_region VARCHAR(255), score DECIMAL(4, 3), gdp_per_capita DECIMAL(4, 3), social_support DECIMAL(4, 3), life_expectancy DECIMAL(4, 3), freedom DECIMAL(4, 3), generosity DECIMAL(4, 3), corruption DECIMAL(4, 3))"
    )
    hiveCtx.sql("INSERT OVERWRITE TABLE happy_2019_1E SELECT * FROM tmp_data")

    // Create partitioned table
    hiveCtx.sql(
      "CREATE TABLE IF NOT EXISTS happy_2019_1E_part(overall_rank INT, score DECIMAL(4, 3), gdp_per_capita DECIMAL(4, 3), social_support DECIMAL(4, 3), life_expectancy DECIMAL(4, 3), freedom DECIMAL(4, 3), generosity DECIMAL(4, 3), corruption DECIMAL(4, 3)) PARTITIONED BY (country_region STRING) row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES(\"skip.header.line.count\"=\"1\")"
    )

    // Insert data into partitioned table
    hiveCtx.sql(
      "INSERT OVERWRITE TABLE happy_2019_1E_part SELECT overall_rank, score, gdp_per_capita, social_support, life_expectancy, freedom, generosity, corruption, country_region FROM happy_2019_1E"
    )

    // Create bucketed table
    hiveCtx.sql(
      "CREATE TABLE IF NOT EXISTS happy_2019_1E_bucket(overall_rank INT, score DECIMAL(4, 3), gdp_per_capita DECIMAL(4, 3), social_support DECIMAL(4, 3), life_expectancy DECIMAL(4, 3), freedom DECIMAL(4, 3), generosity DECIMAL(4, 3), corruption DECIMAL(4, 3)) PARTITIONED BY (country_region STRING) CLUSTERED BY(overall_rank) INTO 4 BUCKETS row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES(\"skip.header.line.count\"=\"1\")"
    )

    // Insert data into partitioned table
    hiveCtx.sql(
      "INSERT OVERWRITE TABLE happy_2019_1E_bucket SELECT overall_rank, score, gdp_per_capita, social_support, life_expectancy, freedom, generosity, corruption, country_region FROM happy_2019_1E"
    )

  }

  def getQueryChoice(hiveCtx: HiveContext): Unit = {
    print(
      "\n\n-- QUERY POINTS --\n\n* Top 10 happiest countries        [1]\n* Countries with highest GDP       [2]\n* Countries with the most freedom  [3]\n* Countries with the least crime   [4]\n* Healthy life expectancy          [5]\n* Most generous countries          [6]\n\nEnter your selection here: "
    )

    var s = new Scanner(System.in)
    var query_choice = s.nextInt()
    if (query_choice == 1) {
      topTen(hiveCtx)
    } else if (query_choice == 2) {
      topGDP(hiveCtx)
    } else if (query_choice == 3) {
      topFreedom(hiveCtx)
    } else if (query_choice == 4) {
      leastCrime(hiveCtx)
    } else if (query_choice == 5) {
      lifeExpectancy(hiveCtx)
    } else if (query_choice == 6) {
      mostGenerous(hiveCtx)
    } else if (query_choice == 7) {
      updateCredentials(hiveCtx)
    } else if (query_choice == 8) {
      deleteUser(hiveCtx)
    }
  }

  def topTen(hiveCtx: HiveContext): Unit = {

    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, score Score FROM happy_2019_1E ORDER BY overall_rank LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/topTen")
    getQueryChoice(hiveCtx)
  }

  def topGDP(hiveCtx: HiveContext): Unit = {

    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, gdp_per_capita Highest_GDP FROM happy_2019_1E ORDER BY Highest_GDP DESC LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/topGDP")
    getQueryChoice(hiveCtx)
  }

  def topFreedom(hiveCtx: HiveContext): Unit = {

    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, freedom Freedom_to_Make_Life_Choices FROM happy_2019_1E ORDER BY Freedom_to_Make_Life_Choices DESC LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/topFreedom")
    getQueryChoice(hiveCtx)
  }

  def leastCrime(hiveCtx: HiveContext): Unit = {

    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, corruption Corruption FROM happy_2019_1E ORDER BY Corruption DESC LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/Corruption")
    getQueryChoice(hiveCtx)
  }

  def lifeExpectancy(hiveCtx: HiveContext): Unit = {
    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, life_expectancy Highest_Life_Expectancy FROM happy_2019_1E ORDER BY Highest_Life_Expectancy DESC LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/lifeExpectancy")
    getQueryChoice(hiveCtx)
  }

  def mostGenerous(hiveCtx: HiveContext): Unit = {
    val result = hiveCtx.sql(
      "SELECT overall_rank Rank, country_region Country, generosity Generosity FROM happy_2019_1E ORDER BY generosity DESC LIMIT 10"
    )
    result.show()
    result.write.mode("overwrite").csv("results/Generosity")
    getQueryChoice(hiveCtx)
  }

  def updateCredentials(hiveCtx: HiveContext): Unit = {
    //This block to connect to mySQL
    val driver = "com.mysql.cj.jdbc.Driver"
    val url =
      "jdbc:mysql://localhost:3306/p1"
    val username = "root"
    val password = "70Sevenkawkroot!"
    var connection: Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    val statement2 = connection.createStatement()
    var s = new Scanner(System.in)
    var table_name = ""

    print("\n\nUPDATE YOUR LOGIN CREDENTIALS\n")
    print("..................................\n\n")
    print("\n* Enter your CURRENT username: ")

    var old_user = s.nextLine().trim()

    print("* Enter your NEW username: ")
    var new_user = s.nextLine().trim()
    print("              \n***\n         ")
    print("\n* Enter your CURRENT password: ")
    var old_password = s.nextLine().trim()

    print("* Enter your NEW password: ")
    var new_password = s.nextLine().trim()

    val resultSet = statement.executeQuery(
      "SELECT COUNT(*) FROM admin_accounts WHERE username='" + old_user + "' AND password='" + old_password + "';"
    )

    while (resultSet.next()) {
      if (resultSet.getString(1) == "1") {
        table_name = "admin_accounts"
      }
    }

    val resultSet2 = statement2.executeQuery(
      "SELECT COUNT(*) FROM user_accounts WHERE username='" + old_user + "' AND password='" + old_password + "';"
    )
    while (resultSet2.next()) {
      if (resultSet2.getString(1) == "1") {
        table_name = "user_accounts"

      }
    }

    val resultSet3 = statement.executeUpdate(
      "UPDATE " + table_name + " SET username ='" + new_user + "' WHERE username ='" + old_user + "' AND password ='" + old_password + "';"
    )

    val resultSet4 = statement.executeUpdate(
      "UPDATE " + table_name + " SET password = '" + new_password + "' WHERE username ='" + new_user + "' AND password ='" + old_password + "';"
    )

    println(".........................")
    println("    UPDATE SUCCESSFUL")
    println(".........................")
    getQueryChoice(hiveCtx)
  }

  def deleteUser(hiveCtx: HiveContext): Unit = {
    //This block to connect to mySQL
    val driver = "com.mysql.cj.jdbc.Driver"
    val url =
      "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
    val username = "root"
    val password = "70Sevenkawkroot!" // Update to include your password
    var connection: Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    val statement2 = connection.createStatement()
    var s = new Scanner(System.in)
    var admin_credentials = ""

    print(
      "\n\nTO DELETE A USER PLEASE \nVERIFY YOUR ADMIN CREDENTIALS\n.................................\n\n"
    )
    print("\n* Enter your username: ")
    var admin_user = s.nextLine().trim()

    print("* Enter your password: ")
    var admin_password = s.nextLine().trim()

    val resultSet = statement2.executeQuery(
      "SELECT COUNT(*) FROM admin_accounts WHERE username='" + admin_user + "' AND password='" + admin_password + "';"
    )

    // Admin table
    while (resultSet.next()) {
      if (resultSet.getString(1) == "1") {
        admin_credentials = "yes"
      }
    }

    val resultSet2 = statement2.executeQuery(
      "SELECT COUNT(*) FROM user_accounts WHERE username='" + admin_user + "' AND password='" + admin_password + "';"
    )

    // User table
    while (resultSet2.next()) {
      if (resultSet2.getString(1) == "1") {
        admin_credentials = "no"
      }
    }

    if (admin_credentials == "yes") {
      print("\n\n* VERIFIED - which user would you like to delete? ")
      var nix_user = s.nextLine().trim()

      // Delete user
      statement.executeUpdate(
        "DELETE FROM user_accounts WHERE username ='" + nix_user + "';"
      )
      println("\n\n.........................")
      println("      USER DELETED")
      println(".........................\n\n")

    } else {
      print("\n\n** You don't have sufficient priviliges **\n\n")
    }

    getQueryChoice(hiveCtx)
  }

}
