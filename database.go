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

func DatabaseConnect() *sql.DB {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s " + "password=%s dbname=%s sslmode=disable",
  host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlInfo)
  Check(err)
  return db
}

func DatabaseEntry(data []Block) {
  db := DatabaseConnect()
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

func DatabaseMenu() {
  db := DatabaseConnect()
  defer db.Close()
  quantityBlocks := QuantityBlocks(db)
  quantityTransactions := QuantityTransactions(db)
  feeTotalSatoshi := FeeTotalSatoshi(db)
  feeTotalUsd := FeeTotalUsd(db)
  fmt.Printf("Количество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(сатоши): %v\nСредняя комиссия за транзакцию(USD): %.2f", quantityBlocks, quantityTransactions, feeTotalSatoshi, feeTotalUsd)
}

func DatabaseClear() {
  db := DatabaseConnect()
  defer db.Close()
  sqlStatement := `DELETE FROM blocks;`
  _, err := db.Exec(sqlStatement)
  Check(err)
}

/*func DatabaseLastRecordTime() time.Time {
  db := DatabaseConnect()
  defer db.Close()
  sqlStatement := `SELECT max(time) FROM blocks;`
  lastString := Query(db, sqlStatement)
  if lastString == "" {
    lastTime, err := time.Parse("2006-01-02 15:04:05", "2001-01-01 12:00:00")
    if err != nil {
        fmt.Println("DatabaseLastRecordTime: parse time error", err)
    }
    return lastTime
  }
  lastString = lastString[1:(len(lastString)-1)]
  lastTime, err := time.Parse("2006-01-02 15:04:05", lastString)
  if err != nil {
      fmt.Println("DatabaseLastRecordTime: parse time error", err)
  }
  return lastTime
}

func DatabaseDeleteOldBlocks(timePastTime time.Time) {
  db := DatabaseConnect()
  defer db.Close()
  timeLastDb := DatabaseLastRecordTime()
  if timeLastDb.After(timePastTime) {
		sqlStatement := `DELETE FROM blocks WHERE id < (SELECT id FROM blocks WHERE time = $1);`
    _, err := db.Exec(sqlStatement, timeLastDb)
    if err != nil {
      panic(err)
    }

	} else {
    sqlStatement := `DELETE FROM blocks;`
    _, err := db.Exec(sqlStatement)
    if err != nil {
      panic(err)
    }
  }
}*/
