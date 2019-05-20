package main

import (
    "fmt"
    "time"
    "strings"
    "strconv"
    "database/sql"
    _ "github.com/lib/pq"
)

const (
  host      = "localhost"
  port      = 5432
  user 	    = "postgres"
  password  = "v19951162020"
  dbname 	  = "bitcoin"
)

const (
  BTC = 100000000.0
  MB = 1000000
)

func DbConnect() *sql.DB {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s " + "password=%s dbname=%s sslmode=disable",
  host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlInfo)
  Check(err)
  return db
}

func DbEntry(data []Block) {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `INSERT INTO blocks (id, time, median_time, size, difficulty, transaction_count, input_count, output_count, input_total, input_total_usd, output_total, output_total_usd, fee_total, fee_total_usd, generation, generation_usd, reward, reward_usd)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`
  for block := 0; block < len(data); block++ {
    _, err := db.Exec(sqlStatement, data[block].attribute...)
    Check(err)
  }
}

func DbQueryLastDay(db *sql.DB, sqlStatement, timeNowTime, timePastTime string) string {
  row := db.QueryRow(sqlStatement, timeNowTime, timePastTime)
  var result string
  err := row.Scan(&result)
  Check(err)
  return result
}

func DbQuery(db *sql.DB, sqlStatement string) string {
  row := db.QueryRow(sqlStatement)
  var result string
  err := row.Scan(&result)
  Check(err)
  return result
}

func CountBlocks(db *sql.DB, timeNowTime, timePastTime string) int {
  sqlStatement := `SELECT count(*) FROM blocks WHERE time < $1 AND time > $2 ;`
  countBlocks, err := strconv.Atoi(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime))
  Check(err)
  return countBlocks
}

func CountTransactions(db *sql.DB, timeNowTime, timePastTime string) int64 {
  sqlStatement := `SELECT sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  countTransactions, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return countTransactions
}

func FeeTotalUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(fee_total_usd)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return feeTotalUSD
}

func FeeTotalBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(fee_total)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalSatoshi, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return feeTotalSatoshi/BTC
}

func AvgTimeBetweenBlocks(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT count(*) FROM blocks WHERE time < $1 AND time > $2;`
  countBlocks, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  sqlStatement = `SELECT max(time) FROM blocks WHERE time < $1 AND time > $2;`
  timeNow := DbStringToTime(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime))
  sqlStatement = `SELECT min(time) FROM blocks WHERE time < $1 AND time > $2;`
  timePast := DbStringToTime(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime))
  timeDiff := float64(timeNow.Sub(timePast)/time.Second)
  avgTime := timeDiff/float64(countBlocks-1)
  return avgTime
}

func SizeMB(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT avg(size) FROM blocks WHERE time < $1 AND time > $2;`
  size, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return size/MB
}

func InputCount(db *sql.DB, timeNowTime, timePastTime string) int64 {
  sqlStatement := `SELECT sum(input_count) FROM blocks WHERE time < $1 AND time > $2;`
  inputCount, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return inputCount
}

func OutputCount(db *sql.DB, timeNowTime, timePastTime string) int64 {
  sqlStatement := `SELECT sum(output_count) FROM blocks WHERE time < $1 AND time > $2;`
  outputCount, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return outputCount
}

func InputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(input_total) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalSatoshi, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return float64(inputTotalSatoshi)/BTC
}

func OutputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(output_total) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalSatoshi, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return float64(outputTotalSatoshi)/BTC
}

func InputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(input_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return inputTotalUSD
}

func OutputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(output_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return outputTotalUSD
}

func GenerationBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(generation) FROM blocks WHERE time < $1 AND time > $2;`
  generationSatoshi, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return generationSatoshi/BTC
}

func GenerationUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(generation_usd) FROM blocks WHERE time < $1 AND time > $2;`
  generationUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return generationUSD
}

func RewardBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(reward) FROM blocks WHERE time < $1 AND time > $2;`
  rewardSatoshi, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return float64(rewardSatoshi)/BTC
}

func RewardUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(reward_usd) FROM blocks WHERE time < $1 AND time > $2;`
  rewardUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return rewardUSD
}

func DbStat(timeNowTime, timePastTime string) []interface{} {
  db := DbConnect()
  defer db.Close()
  data := make([]interface{}, 0)
  data = append(data, CountBlocks(db, timeNowTime, timePastTime), CountTransactions(db, timeNowTime, timePastTime), FeeTotalBTC(db, timeNowTime, timePastTime), FeeTotalUSD(db, timeNowTime, timePastTime), AvgTimeBetweenBlocks(db, timeNowTime, timePastTime), SizeMB(db, timeNowTime, timePastTime), InputCount(db, timeNowTime, timePastTime), OutputCount(db, timeNowTime, timePastTime), InputTotalBTC(db, timeNowTime, timePastTime), OutputTotalBTC(db, timeNowTime, timePastTime), InputTotalUSD(db, timeNowTime, timePastTime), OutputTotalUSD(db, timeNowTime, timePastTime), GenerationBTC(db, timeNowTime, timePastTime), GenerationUSD(db, timeNowTime, timePastTime), RewardBTC(db, timeNowTime, timePastTime), RewardUSD(db, timeNowTime, timePastTime))
  return data
}

func DbEmpty() bool {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `SELECT count(*) FROM blocks;`
  countBlocks, err := strconv.Atoi(DbQuery(db, sqlStatement))
  Check(err)
  if countBlocks == 0 {
    return true
  }
  return false
}

func DbStringToTime(timeString string) time.Time {
	timeString = timeString[0:len(timeString)-1]
  timeString = strings.Join(strings.Split(timeString, "T"), " ")
	time, err := time.Parse("2006-01-02 15:04:05", timeString)
	Check(err)
	return time
}

func DbLastTime() time.Time {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `SELECT max(time) FROM blocks;`
  timeLastDb := DbStringToTime(DbQuery(db, sqlStatement))
  return timeLastDb
}
