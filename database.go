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

func StringToIntCheckErr(valueStr string, message string, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	valueInt, err := strconv.ParseInt(valueStr, 10, 64)
  if err != nil {
    return 0, Wrap(Wrapf(err, "StringToIntCheckErr: Ошибка конвертации string в int64, значения %v", valueStr), message)
  }
	return valueInt, nil
}

func StringToFloatCheckErr(valueStr string, message string, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	valuefloat, err := strconv.ParseFloat(valueStr, 64)
  if err != nil {
    return 0, Wrap(Wrapf(err, "StringToFloatCheckErr: Ошибка конвертации string в float64, значения %v", valueStr), message)
  }
	return valuefloat, nil
}

func DoFuncCheckErr(fn func(*sql.DB, string, string, string) (string, error), db *sql.DB, sqlStatement string, timeNow string, timePast string, message string) (string, error) {

  var result string
  result, err := fn(db, sqlStatement, timeNow, timePast)
  if err != nil {
    return "", Wrap(Wrap(err, "DoFuncCheckErr"), message)
  }
  return result, nil
}

func DoManyFuncs(db *sql.DB, timeNow string, timePast string, message string, fncs ...func(*sql.DB, string, string) (interface{}, error)) ([]interface{}, error) {

  data := make([]interface{}, 0)
  for _, fn := range fncs {
    value, err := fn(db, timeNow, timePast)
    if err != nil {
      return data, Wrap(Wrap(err, "DoManyFuncs"), message)
    }
    data = append(data, value)
  }
  return data, nil
}

func DbConnect() (*sql.DB, error) {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s " + "password=%s dbname=%s sslmode=disable",
  host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    return db, Wrap(err, "DbConnect: ошибка подключения к базе данных.")
  }
  return db, nil
}

func DbEntry(data []Block) error {
  var err error
  db, err := DbConnect()
  defer db.Close()
  if err != nil {
    return Wrap(err, "DbEntry")
  }
  sqlStatement := `INSERT INTO blocks (id, time, median_time, size, difficulty, transaction_count, input_count, output_count, input_total, input_total_usd, output_total, output_total_usd, fee_total, fee_total_usd, generation, generation_usd, reward, reward_usd)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`
  for block, _ := range data {
    _, err := db.Exec(sqlStatement, data[block].attributes...)
    if err != nil {
      return Wrapf(err, "DbEntry: Ошибка при записи данных в таблицу: %v", data[block].attributes...)
    }
  }
  return nil
}

func DbQueryDay(db *sql.DB, sqlStatement, timeNowTime, timePastTime string) (string, error) {
  row := db.QueryRow(sqlStatement, timeNowTime, timePastTime)
  var result string
  err := row.Scan(&result)
  if err != nil {
    return "", Wrapf(err, "DbQueryLastDay: Ошибка sql запроса, при получении данных: %v", result)
  }
  return result, nil
}

func DbQuery(db *sql.DB, sqlStatement string) (string, error) {
  row := db.QueryRow(sqlStatement)
  var result string
  err := row.Scan(&result)
  if err != nil {
    return "", Wrapf(err, "DbQuery: Ошибка sql запроса, при получении данных: %v", result)
  }
  return result, nil
}

func CountBlocks(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT count(*) FROM blocks WHERE time < $1 AND time > $2 ;`
  countBlocks, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "CountBlocks")
  return StringToIntCheckErr(countBlocks, "CountBlocks", err)
}

func CountTransactions(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  countTransactions, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "CountTransactions")
  return StringToIntCheckErr(countTransactions, "CountTransactions", err)
}

func FeeTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(fee_total_usd)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "FeeTotalUSD")
  return StringToFloatCheckErr(feeTotalUSD, "FeeTotalUSD", err)
}

func FeeTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(fee_total)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "FeeTotalBTC")
  feeTotalSatoshi, err := StringToFloatCheckErr(feeTotalSatoshiString, "FeeTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(feeTotalSatoshi)/BTC, nil
}

func MaxTimeDay(db *sql.DB, timeNowTime, timePastTime string) (time.Time, error) {
  sqlStatement := `SELECT max(time) FROM blocks WHERE time < $1 AND time > $2;`
  timeNowString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "MaxTimeDay")
  if err != nil {
    return time.Now(), err
  }
  timeNow, err := DbStringToTime(timeNowString)
  if err != nil {
    return timeNow, Wrap(err, "MaxTimeDay")
  }
  return timeNow, nil
}

func MinTimeDay(db *sql.DB, timeNowTime, timePastTime string) (time.Time, error) {
  sqlStatement := `SELECT min(time) FROM blocks WHERE time < $1 AND time > $2;`
  timePastString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "MinTimeDay")
  if err != nil {
    return time.Now(), err
  }
  timePast, err := DbStringToTime(timePastString)
  if err != nil {
    return timePast, Wrap(err, "MinTimeDay")
  }
  return timePast, nil
}

func AvgTimeBetweenBlocks(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  countBlocks, err := CountBlocks(db, timeNowTime, timePastTime)
  if err != nil {
    return 0.0, Wrap(err, "AvgTimeBetweenBlocks")
  }
  timeMax, err := MaxTimeDay(db, timeNowTime, timePastTime)
  if err != nil {
    return 0.0, Wrap(err, "AvgTimeBetweenBlocks")
  }
  timeMin, err := MinTimeDay(db, timeNowTime, timePastTime)
  if err != nil {
    return 0.0, Wrap(err, "AvgTimeBetweenBlocks")
  }
  timeDiff := timeMax.Sub(timeMin)/time.Second
  countBlocksInt := countBlocks.(int64)
  return float64(timeDiff)/float64(countBlocksInt-1), nil
}

func SizeMB(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT avg(size) FROM blocks WHERE time < $1 AND time > $2;`
  size, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "SizeMB")
  return StringToFloatCheckErr(size, "SizeMB", err)
}

func InputCount(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_count) FROM blocks WHERE time < $1 AND time > $2;`
  inputCount, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputCount")
  return StringToIntCheckErr(inputCount, "InputCount", err)
}

func OutputCount(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_count) FROM blocks WHERE time < $1 AND time > $2;`
  outputCount, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputCount")
  return StringToIntCheckErr(outputCount, "OutputCount", err)
}

func InputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_total) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputTotalBTC")
  inputTotalSatoshi, err := StringToIntCheckErr(inputTotalSatoshiString, "InputTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(inputTotalSatoshi)/BTC, nil
}

func OutputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_total) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputTotalBTC")
  outputTotalSatoshi, err := StringToIntCheckErr(outputTotalSatoshiString, "OutputTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(outputTotalSatoshi)/BTC, nil
}

func InputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputTotalUSD")
  return StringToFloatCheckErr(inputTotalUSD, "InputTotalUSD", err)
}

func OutputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputTotalUSD")
  return StringToFloatCheckErr(outputTotalUSD, "OutputTotalUSD", err)
}

func GenerationBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(generation) FROM blocks WHERE time < $1 AND time > $2;`
  generationSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "GenerationBTC")
  generationSatoshi, err := StringToIntCheckErr(generationSatoshiString, "GenerationBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(generationSatoshi)/BTC, nil
}

func GenerationUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(generation_usd) FROM blocks WHERE time < $1 AND time > $2;`
  generationUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "GenerationUSD")
  return StringToFloatCheckErr(generationUSD, "GenerationUSD", err)
}

func RewardBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(reward) FROM blocks WHERE time < $1 AND time > $2;`
  rewardSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "RewardBTC")
  rewardSatoshi, err := StringToIntCheckErr(rewardSatoshiString, "RewardBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(rewardSatoshi)/BTC, nil
}

func RewardUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(reward_usd) FROM blocks WHERE time < $1 AND time > $2;`
  rewardUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "RewardUSD")
  return StringToFloatCheckErr(rewardUSD, "RewardUSD", err)
}

func DbStat(timeNow, timePast string) ([]interface{}, error) {
  db, err := DbConnect()
  defer db.Close()
  if err != nil {
    return make([]interface{}, 0), Wrap(err, "DbStat")
  }
  data, err := DoManyFuncs(db, timeNow, timePast, "DbStat", CountBlocks, CountTransactions, FeeTotalBTC, FeeTotalUSD, AvgTimeBetweenBlocks, SizeMB, InputCount, OutputCount, InputTotalBTC, OutputTotalBTC, InputTotalUSD, OutputTotalUSD, GenerationBTC, GenerationUSD, RewardBTC, RewardUSD)
  return data, err
}

func DbEmpty() (bool, error) {
  db, err := DbConnect()
  defer db.Close()
  if err != nil {
    return false, Wrap(err, "DbEmpty")
  }
  sqlStatement := `SELECT count(*) FROM blocks;`
  countBlocksString, err := DbQuery(db, sqlStatement)
  if err != nil {
    return false, Wrap(err, "DbEmpty")
  }
  countBlocks, err := StringToIntCheckErr(countBlocksString, "DbEmpty", err)
  if countBlocks == 0 {
    return true, err
  }
  return false, err
}

func DbStringToTime(timeString string) (time.Time, error) {
	timeString = timeString[0:len(timeString)-1]
  timeString = strings.Join(strings.Split(timeString, "T"), " ")
	timeTime, err := time.Parse("2006-01-02 15:04:05", timeString)
  if err != nil {
    return time.Now(), Wrap(err, "DbEmpty: Ошибка конвертации string в time.Time")
  }
	return timeTime, nil
}

func DbLastTime() (time.Time, error) {
  db, err := DbConnect()
  defer db.Close()
  if err != nil {
    return time.Now(), Wrap(err, "DbLastTime")
  }
  sqlStatement := `SELECT max(time) FROM blocks;`
  timeLastDbString, err := DbQuery(db, sqlStatement)
  if err != nil {
    return time.Now(), Wrap(err, "DbLastTime")
  }
  timeLastDb, err := DbStringToTime(timeLastDbString)
  if err != nil {
    return time.Now(), Wrap(err, "DbLastTime")
  }
  return timeLastDb, nil
}
