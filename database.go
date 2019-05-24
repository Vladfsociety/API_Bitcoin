package main

import (
    "fmt"
    "time"
    "strings"
    "strconv"
    "database/sql"
    _ "github.com/lib/pq"
)

// Данные для подключения к бд.
const (
  host      = "localhost"
  port      = 5432
  user 	    = "postgres"
  password  = "v19951162020"
  dbname 	  = "bitcoin"
)

const (
  BTC = 100000000.0 // биткоин в сатоши.
  MB = 1000000.0 // мегабайт в байтах.
)

// Функция, которая с одной стороны преобразует строковое значение, полученное из базы в флоат, а с другой - проверяет ошибку на входе(Хотел хоть как-то избавитсья от ифов).
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

// Функция, которая с одной стороны преобразует строковое значение, полученное из базы в инт, а с другой - проверяет ошибку на входе(Хотел хоть как-то избавитсья от ифов).
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

// Выполняет функцию запроса в базу и обворачивает возможную ошибку.
func DoFuncCheckErr(fn func(*sql.DB, string, string, string) (string, error), db *sql.DB, sqlStatement string, timeNow string, timePast string, message string) (string, error) {

  result, err := fn(db, sqlStatement, timeNow, timePast)
  if err != nil {
    return "", Wrap(Wrap(err, "DoFuncCheckErr"), message)
  }
  return result, nil
}

// Функция выполняющая все необходимые функции, для получения статистики и записывающая данные в data. У всех функций выполняющих запрос возвращаемым значением был пустой интерфейс, чтобы можно было их передать аргументами и можно было считать разнотипные данные в 1 слайс.
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

// Подключение к базе.
func DbConnect() (*sql.DB, error) {
  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s " + "password=%s dbname=%s sslmode=disable",
  host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    return db, Wrap(err, "DbConnect: ошибка подключения к базе данных.")
  }
  return db, nil
}

// Функция записи данных в таблицу.
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

// Функция запроса к базе с временными рамками.
func DbQueryDay(db *sql.DB, sqlStatement, timeNowTime, timePastTime string) (string, error) {
  row := db.QueryRow(sqlStatement, timeNowTime, timePastTime)
  var result string
  err := row.Scan(&result)
  if err != nil {
    return "", Wrapf(err, "DbQueryLastDay: Ошибка sql запроса, при получении данных: %v", result)
  }
  return result, nil
}

// Функция запроса к базе без временных рамок.
func DbQuery(db *sql.DB, sqlStatement string) (string, error) {
  row := db.QueryRow(sqlStatement)
  var result string
  err := row.Scan(&result)
  if err != nil {
    return "", Wrapf(err, "DbQuery: Ошибка sql запроса, при получении данных: %v", result)
  }
  return result, nil
}

// Количество блоков.
func CountBlocks(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT count(*) FROM blocks WHERE time < $1 AND time > $2 ;`
  countBlocks, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "CountBlocks")
  return StringToIntCheckErr(countBlocks, "CountBlocks", err)
}

// Количество транзакций.
func CountTransactions(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  countTransactions, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "CountTransactions")
  return StringToIntCheckErr(countTransactions, "CountTransactions", err)
}

// Средняя комиссия за транзакцию в USD.
func FeeTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(fee_total_usd)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "FeeTotalUSD")
  return StringToFloatCheckErr(feeTotalUSD, "FeeTotalUSD", err)
}

// Средняя комиссия за транзакцию в BTC.
func FeeTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(fee_total)/sum(transaction_count) FROM blocks WHERE time < $1 AND time > $2 ;`
  feeTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "FeeTotalBTC")
  feeTotalSatoshi, err := StringToFloatCheckErr(feeTotalSatoshiString, "FeeTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(feeTotalSatoshi)/BTC, nil
}

// Время нахождения последнего блока за последние 24 часа.
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

// Время нахождения первого блока за последние 24 часа.
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

// Среднее время между блоками.
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

// Средний размер блока в мегабайтах.
func SizeMB(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT avg(size) FROM blocks WHERE time < $1 AND time > $2;`
  sizeString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "SizeMB")
  size, err := StringToFloatCheckErr(sizeString, "SizeMB", err)
  if err != nil {
    return 0.0, err
  }
  return float64(size)/MB, nil
}

// Количество входов всех транзакций во всех блоках.
func InputCount(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_count) FROM blocks WHERE time < $1 AND time > $2;`
  inputCount, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputCount")
  return StringToIntCheckErr(inputCount, "InputCount", err)
}

// Количество выходов всех транзакций во всех блоках.
func OutputCount(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_count) FROM blocks WHERE time < $1 AND time > $2;`
  outputCount, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputCount")
  return StringToIntCheckErr(outputCount, "OutputCount", err)
}

// Сумма входов всех транзакций во всех блоках в BTC.
func InputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_total) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputTotalBTC")
  inputTotalSatoshi, err := StringToIntCheckErr(inputTotalSatoshiString, "InputTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(inputTotalSatoshi)/BTC, nil
}

// Сумма выходов всех транзакций во всех блоках в BTC.
func OutputTotalBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_total) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputTotalBTC")
  outputTotalSatoshi, err := StringToIntCheckErr(outputTotalSatoshiString, "OutputTotalBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(outputTotalSatoshi)/BTC, nil
}

// Сумма входов всех транзакций во всех блоках в USD.
func InputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(input_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  inputTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "InputTotalUSD")
  return StringToFloatCheckErr(inputTotalUSD, "InputTotalUSD", err)
}

// Сумма выходов всех транзакций во всех блоках в USD.
func OutputTotalUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(output_total_usd) FROM blocks WHERE time < $1 AND time > $2;`
  outputTotalUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "OutputTotalUSD")
  return StringToFloatCheckErr(outputTotalUSD, "OutputTotalUSD", err)
}

// Нахождение суммарной награды майнеров за нахождение блоков в BTC.
func GenerationBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(generation) FROM blocks WHERE time < $1 AND time > $2;`
  generationSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "GenerationBTC")
  generationSatoshi, err := StringToIntCheckErr(generationSatoshiString, "GenerationBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(generationSatoshi)/BTC, nil
}

// Нахождение суммарной награды майнеров за нахождение блоков в USD.
func GenerationUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(generation_usd) FROM blocks WHERE time < $1 AND time > $2;`
  generationUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "GenerationUSD")
  return StringToFloatCheckErr(generationUSD, "GenerationUSD", err)
}

// Нахождение суммарной награды майнеров(за блок + комиссия) в BTC.
func RewardBTC(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(reward) FROM blocks WHERE time < $1 AND time > $2;`
  rewardSatoshiString, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "RewardBTC")
  rewardSatoshi, err := StringToIntCheckErr(rewardSatoshiString, "RewardBTC", err)
  if err != nil {
    return 0.0, err
  }
  return float64(rewardSatoshi)/BTC, nil
}

// Нахождение суммарной награды майнеров(за блок + комиссия) в USD.
func RewardUSD(db *sql.DB, timeNowTime, timePastTime string) (interface{}, error) {
  sqlStatement := `SELECT sum(reward_usd) FROM blocks WHERE time < $1 AND time > $2;`
  rewardUSD, err := DoFuncCheckErr(DbQueryDay, db, sqlStatement, timeNowTime, timePastTime, "RewardUSD")
  return StringToFloatCheckErr(rewardUSD, "RewardUSD", err)
}

// Получение статистики из бд и запись в слайс.
func DbStat(timeNow, timePast string) ([]interface{}, error) {
  db, err := DbConnect()
  defer db.Close()
  if err != nil {
    return make([]interface{}, 0), Wrap(err, "DbStat")
  }
  data, err := DoManyFuncs(db, timeNow, timePast, "DbStat", CountBlocks, CountTransactions, FeeTotalBTC, FeeTotalUSD, AvgTimeBetweenBlocks, SizeMB, InputCount, OutputCount, InputTotalBTC, OutputTotalBTC, InputTotalUSD, OutputTotalUSD, GenerationBTC, GenerationUSD, RewardBTC, RewardUSD)
  return data, err
}

// Проверка пуста ли бд.
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

// Возвращает время нахождения последнего блока из бд.
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
