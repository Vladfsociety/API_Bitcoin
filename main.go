package main

import (
		"log"
		"fmt"
		"time"
		"strings"
		"strconv"
)

const(
	deltaUTC = 3 * time.Hour
	oneDay = 24 * time.Hour
	waitTime = 20 * time.Second
	deltaDb = 1 * time.Second
)

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

func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05")
}

func GetTime() (time.Time, time.Time) {
	timeNow := time.Now()
	timeNowUTC := timeNow.Add(-deltaUTC)
	return timeNowUTC, timeNowUTC.Add(-oneDay)
}

func GetTimeResult(timeNowUTC, timePastUTC time.Time) (string, error) {
	var err error
	empty, err := DbEmpty()
	if err != nil {
		return "", Wrap(err, "GetTimeResult")
	}
	if !empty {
		dbLastTime, err := DbLastTime()
		if err != nil {
			return "", Wrap(err, "GetTimeResult")
		}
		timePastUTC = dbLastTime.Add(deltaDb)
  }
	timeNowUTCString := TimeToString(timeNowUTC)
	timePastUTCString := TimeToString(timePastUTC)
  timeResult := strings.Join([]string{timePastUTCString, timeNowUTCString}, "..")
	return timeResult, nil
}

func FirstStartAfterOff() {
	var err error
	timeNowUTC, timePastUTC := GetTime()
	timeResult, err := GetTimeResult(timeNowUTC, timePastUTC)
	if err != nil {
		log.Fatal(err)
	}
	data, err := GetData(timeResult)
	if err != nil {
		log.Fatal(err)
	}
	err = DbEntry(data)
	if err != nil {
		log.Fatal(err)
	}
}

func GetDataAndEntryInDb(ch chan int) {
	for {
		time.Sleep(waitTime)
		var err error
		timeNowUTC, timePastUTC := GetTime()
		fmt.Println("\n", timeNowUTC)
		timeResult, err := GetTimeResult(timeNowUTC, timePastUTC)
		if err != nil {
			log.Fatal(err)
		}
		data, err := GetData(timeResult)
		if err != nil {
			log.Fatal(err)
		}
		<- ch
		err = DbEntry(data)
		ch <- 1
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	FirstStartAfterOff()
	ch := make(chan int, 1)
	fmt.Println("Для вывода статистики нажмите 1, для выхода нажмите 2.")
	var input int
	ch <- 1
	go GetDataAndEntryInDb(ch)
	for {
		fmt.Scan(&input)
		switch input {
		case 1:
			timeNowUTC, timePastUTC := GetTime()
			<- ch
			data, err := DbStat(TimeToString(timeNowUTC), TimeToString(timePastUTC))
			ch <- 1
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(data)
			fmt.Printf("Статистика за последние 24 часа:\nКоличество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(BTC): %.8f\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f\nСредний размер блока(мбайты): %.2f\nКоличество входов во всех транзакциях всех блоков: %v\nКоличество выходов во всех транзакциях всех блоков: %v\nСумма входов во всех блоках(BTC): %.2f\nСумма выходов во всех блоках(BTC): %.2f\nСумма входов во всех блоках(USD): %.2f\nСумма выходов во всех блоках(USD): %.2f\nСуммарная награда майнеров за нахождение блоков(BTC): %.2f\nСуммарная награда майнеров за нахождение блоков(USD): %.2f\nСуммарная награда майнеров(за блок + комиссия)(BTC): %.2f\nСуммарная награда майнеров(за блок + комиссия)(USD): %.2f\n", data...)
		case 2:
			return
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}
