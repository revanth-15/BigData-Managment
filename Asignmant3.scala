// Databricks notebook source
// MAGIC %md
// MAGIC #### Create DataFrames from the Soccer data files

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/
// MAGIC

// COMMAND ----------

val matchDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Match_results.csv")
val countryDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Country.csv")
val assistsDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Player_Assists_Goals.csv")
val cardsDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Player_Cards.csv")
val playersDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Players.csv")
val historyDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Worldcup_History.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.	Retrieve the list of country names that have won a world cup.

// COMMAND ----------

historyDF.select("Winner").distinct().show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.	Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

// COMMAND ----------

import org.apache.spark.sql.functions._


// COMMAND ----------

historyDF.groupBy("Winner")
  .count()
  .withColumnRenamed("count", "Titles")
  .orderBy(desc("Titles"))
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.	List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

// COMMAND ----------

countryDF.filter($"population" > 100.00)
  .select("CountryName", "capital", "population")
  .orderBy("population")
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.	List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.

// COMMAND ----------

matchDF.filter($"Team1Score" > 4 || $"Team2Score" > 4)
  .select("Stadium")
  .distinct()
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.	List the names of all the cities which have the name of the Stadium starting with “Estadio”.

// COMMAND ----------

matchDF.filter($"Stadium".startsWith("Estadio"))
  .select("City")
  .distinct()
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.	List all stadiums and the number of matches hosted by each stadium.

// COMMAND ----------

matchDF.groupBy("Stadium")
  .count()
  .withColumnRenamed("count", "MatchesHosted")
  .orderBy(desc("MatchesHosted"))
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.	List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.

// COMMAND ----------

playersDF.filter($"Height" > 198)
  .select("Fname", "Lname", "BirthDate")
  .show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

// Step 1: Inner join playersDF with cardsDF (card info is required)
val playerWithCards = playersDF
  .join(cardsDF, Seq("PID"), "inner") // Keep players who have card records

// Step 2: Left join with assistsDF (some players might not have goal/assist info)
val playerStats = playerWithCards
  .join(assistsDF, Seq("PID"), "left_outer") // Keep all players, even if goals are null

// Step 3: Filter captains with >2 yellow cards OR exactly 1 red card
val filteredCaptains = playerStats.filter(
  $"isCaptain" === true &&
  (
    $"no_of_yellow_cards" > 2 ||
    coalesce($"no_of_red_cards".cast("int"), lit(0)) === 1
  )
)

// Step 4: Select required columns and handle nulls
filteredCaptains
  .select(
    $"Fname",
    $"Lname",
    $"Position",
    coalesce($"goals", lit(0)).alias("goals")
  )
  .show(false)


// COMMAND ----------

matchDF.write
  .option("header", "true")
  .csv("/FileStore/output/match_results_output")


// COMMAND ----------

// MAGIC %fs ls /FileStore/output/
// MAGIC