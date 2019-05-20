package main

import (
    "fmt"
    "time"
    //"strings"
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
  feeTotalUsd, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return feeTotalUsd
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
  timeNow := StringToTime(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime))
  sqlStatement = `SELECT min(time) FROM blocks WHERE time < $1 AND time > $2;`
  timePast := StringToTime(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime))
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
  inputTotalUsd, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return inputTotalUsd
}

func OutputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(output_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalUsd, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return outputTotalUsd
}

func RewardBTC(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(reward) FROM blocks WHERE time < $1 AND time > $2;`
  rewardBTC, err := strconv.ParseInt(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 10, 64)
  Check(err)
  return float64(rewardBTC)/BTC
}

func RewardUSD(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT sum(reward_usd) FROM blocks WHERE time < $1 AND time > $2;`
  rewardUSD, err := strconv.ParseFloat(DbQueryLastDay(db, sqlStatement, timeNowTime, timePastTime), 64)
  Check(err)
  return rewardUSD
}

func DbStat(timeNowTime, timePastTime string) {
  db := DbConnect()
  defer db.Close()
  fmt.Printf("Статистика за последниее 24 часа:\nКоличество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(BTC): %.8f\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f\nСредний размер блока(мбайты): %.2f\nКоличество входов во всех транзакциях блоков: %v\nКоличество выходов во всех транзакциях блоков: %v\nСумма входов во всех блоках(BTC): %.2f\nСумма выходов во всех блоках(BTC): %.2f\nСумма входов во всех блоках(USD): %.2f\nСумма выходов во всех блоках(USD): %.2f\nСуммарная награда майнеров(за блок + комиссия)(BTC): %.2f\nСуммарная награда майнеров(за блок + комиссия)(USD): %.2f\n", CountBlocks(db, timeNowTime, timePastTime), CountTransactions(db, timeNowTime, timePastTime), FeeTotalBTC(db, timeNowTime, timePastTime), FeeTotalUSD(db, timeNowTime, timePastTime), AvgTimeBetweenBlocks(db, timeNowTime, timePastTime), SizeMB(db, timeNowTime, timePastTime), InputCount(db, timeNowTime, timePastTime), OutputCount(db, timeNowTime, timePastTime), InputTotalBTC(db, timeNowTime, timePastTime), OutputTotalBTC(db, timeNowTime, timePastTime), InputTotalUSD(db, timeNowTime, timePastTime), OutputTotalUSD(db, timeNowTime, timePastTime), RewardBTC(db, timeNowTime, timePastTime), RewardUSD(db, timeNowTime, timePastTime))
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

func DbLastTime() time.Time {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `SELECT max(time) FROM blocks;`
  timeLastDb := StringToTime(DbQuery(db, sqlStatement))
  return timeLastDb
}
