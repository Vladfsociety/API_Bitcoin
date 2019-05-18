package main

import (
    "fmt"
    //"time"
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

func DbQuery(db *sql.DB, sqlStatement string) string {
  row := db.QueryRow(sqlStatement)
  var result string
  err := row.Scan(&result)
  Check(err)
  return result
}

func QuantityBlocks(db *sql.DB) int {
  sqlStatement := `SELECT count(*) FROM blocks;`
  quantityBlocksString := DbQuery(db, sqlStatement)
  quantityBlocksInt, err := strconv.Atoi(quantityBlocksString)
  Check(err)
  return quantityBlocksInt
}

func QuantityTransactions(db *sql.DB) int32 {
  sqlStatement := `SELECT sum(transaction_count) FROM blocks;`
  quantityTransactionsString := DbQuery(db, sqlStatement)
  quantityTransactionsInt, err := strconv.ParseInt(quantityTransactionsString, 10, 32)
  Check(err)
  return int32(quantityTransactionsInt)
}

func FeeTotalUsd(db *sql.DB) float32 {
  sqlStatement := `SELECT sum(fee_total_usd)/sum(transaction_count) FROM blocks;`
  FeeTotalUsdString := DbQuery(db, sqlStatement)
  FeeTotalUsdFloat, err := strconv.ParseFloat(FeeTotalUsdString, 32)
  Check(err)
  return float32(FeeTotalUsdFloat)
}

func FeeTotalSatoshi(db *sql.DB) int32 {
  sqlStatement := `SELECT sum(fee_total)/sum(transaction_count) FROM blocks;`
  FeeTotalSatoshiString := DbQuery(db, sqlStatement)
  FeeTotalSatoshiFloat, err := strconv.ParseFloat(FeeTotalSatoshiString, 32)
  Check(err)
  FeeTotalSatoshiInt := int32(FeeTotalSatoshiFloat)
  return FeeTotalSatoshiInt
}

func DbStat() {
  db := DbConnect()
  defer db.Close()
  quantityBlocks := QuantityBlocks(db)
  quantityTransactions := QuantityTransactions(db)
  feeTotalSatoshi := FeeTotalSatoshi(db)
  feeTotalUsd := FeeTotalUsd(db)
  fmt.Printf("Количество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(сатоши): %v\nСредняя комиссия за транзакцию(USD): %.2f", quantityBlocks, quantityTransactions, feeTotalSatoshi, feeTotalUsd)
}

func DbClear() {
  db := DbConnect()
  defer db.Close()
  sqlStatement := `DELETE FROM blocks;`
  _, err := db.Exec(sqlStatement)
  Check(err)
}
