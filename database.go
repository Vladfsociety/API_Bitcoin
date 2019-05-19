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

func DbQueryLastDayRows(db *sql.DB, sqlStatement, timeNowTime, timePastTime string) []string {
  rows, err := db.Query(sqlStatement, timeNowTime, timePastTime)
  Check(err)
  defer rows.Close()
  blocks := make([]string, 0)
  var block string
  for rows.Next() {
    err := rows.Scan(&block)
    Check(err)
    blocks = append(blocks, block)
  }
  err = rows.Err()
  Check(err)
  return blocks
}

func DbQueryLastDayRow(db *sql.DB, sqlStatement, timeNowTime, timePastTime string) string {
  row := db.QueryRow(sqlStatement, timeNowTime, timePastTime)
  var result string
  err := row.Scan(&result)
  Check(err)
  return result
}

func DbQueryRow(db *sql.DB, sqlStatement string) string {
  row := db.QueryRow(sqlStatement)
  var result string
  err := row.Scan(&result)
  Check(err)
  return result
}

func QuantityBlocks(db *sql.DB, timeNowTime, timePastTime string) int {
  sqlStatement := `SELECT count(*) FROM blocks WHERE time < $1 AND time > $2 ;`
  quantityBlocksString := DbQueryLastDayRow(db, sqlStatement, timeNowTime, timePastTime)
  quantityBlocksInt, err := strconv.Atoi(quantityBlocksString)
  Check(err)
  return quantityBlocksInt
}

func QuantityTransactions(db *sql.DB, timeNowTime, timePastTime string) int64 {
  sqlStatement := `SELECT sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  quantityTransactionsString := DbQueryLastDayRow(db, sqlStatement, timeNowTime, timePastTime)
  quantityTransactionsInt, err := strconv.ParseInt(quantityTransactionsString, 10, 64)
  Check(err)
  return quantityTransactionsInt
}

func FeeTotalUsd(db *sql.DB, timeNowTime, timePastTime string) float32 {
  sqlStatement := `SELECT sum(fee_total_usd)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  FeeTotalUsdString := DbQueryLastDayRow(db, sqlStatement, timeNowTime, timePastTime)
  FeeTotalUsdFloat, err := strconv.ParseFloat(FeeTotalUsdString, 32)
  Check(err)
  return float32(FeeTotalUsdFloat)
}

func FeeTotalSatoshi(db *sql.DB, timeNowTime, timePastTime string) int32 {
  sqlStatement := `SELECT sum(fee_total)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  FeeTotalSatoshiString := DbQueryLastDayRow(db, sqlStatement, timeNowTime, timePastTime)
  FeeTotalSatoshiFloat, err := strconv.ParseFloat(FeeTotalSatoshiString, 32)
  Check(err)
  FeeTotalSatoshiInt := int32(FeeTotalSatoshiFloat)
  return FeeTotalSatoshiInt
}

func AvgTimeBetweenBlocks(db *sql.DB, timeNowTime, timePastTime string) float64 {
  sqlStatement := `SELECT median_time FROM blocks WHERE median_time < $1 AND median_time > $2 ORDER BY median_time DESC;`
  timeBlocks := DbQueryLastDayRows(db, sqlStatement, timeNowTime, timePastTime)
  var sumTime int64 = 0
  var timeLater, timeEarlier time.Time
  timeLater = StringToTime(timeBlocks[0])
  for block := 1; block < len(timeBlocks); block++ {
    timeEarlier = StringToTime(timeBlocks[block])
    sumTime += int64(timeLater.Sub(timeEarlier)/time.Second)
    timeLater = timeEarlier
  }
  avgTime := float64(sumTime/int64(len(timeBlocks)-1))
  return avgTime
}

func DbStat(timeNowTime, timePastTime string) {
  db := DbConnect()
  defer db.Close()
  quantityBlocks := QuantityBlocks(db, timeNowTime, timePastTime)
  quantityTransactions := QuantityTransactions(db, timeNowTime, timePastTime)
  feeTotalSatoshi := FeeTotalSatoshi(db, timeNowTime, timePastTime)
  feeTotalUsd := FeeTotalUsd(db, timeNowTime, timePastTime)
  avgtimeBlocks := AvgTimeBetweenBlocks(db, timeNowTime, timePastTime)
  fmt.Printf("Количество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(сатоши): %v\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f", quantityBlocks, quantityTransactions, feeTotalSatoshi, feeTotalUsd, avgtimeBlocks)
}

func DbClear() {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `DELETE FROM blocks;`
  _, err := db.Exec(sqlStatement)
  Check(err)
}

func DbEmpty() bool {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `SELECT count(*) FROM blocks;`
  countString := DbQueryRow(db, sqlStatement)
  countInt, err := strconv.Atoi(countString)
  Check(err)
  if countInt == 0 {
    return true
  }
  return false
}

func DbLastTime() time.Time {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `SELECT max(time) FROM blocks;`
  timeLastString := DbQueryRow(db, sqlStatement)
  timeLast := StringToTime(timeLastString)
  return timeLast
}
