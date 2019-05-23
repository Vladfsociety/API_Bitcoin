package main

import (
		"log"
		"fmt"
		"time"
		"strings"
)

const(
	deltaUTC = 3 * time.Hour
	oneDay = 24 * time.Hour
	waitTime = 20 * time.Second
	deltaDb = 1 * time.Second
)

func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05")
}

func GetTime() (time.Time, time.Time) {
	timeNow := time.Now()
	timeNowUTC := timeNow.Add(-deltaUTC)
	return timeNowUTC, timeNowUTC.Add(-oneDay)
}

func GetValidTimePast(timePastUTC time.Time) (time.Time, error) {
	empty, err := DbEmpty()
	if err != nil {
		return timePastUTC, Wrap(err, "GetValidTimePast")
	}
	if !empty {
		dbLastTime, err := DbLastTime()
		if err != nil {
			return timePastUTC, Wrap(err, "GetValidTimePast")
		}
		return dbLastTime.Add(deltaDb), nil
  }
	return timePastUTC, nil
}

func GetTimeResult() (string, error) {
	timeNowUTC, timePastUTC := GetTime()
	timePastUTC, err := GetValidTimePast(timePastUTC)
	if err != nil {
		return "", Wrap(err, "GetTimeResult")
	}
	timeNowUTCString := TimeToString(timeNowUTC)
	timePastUTCString := TimeToString(timePastUTC)
  timeResult := strings.Join([]string{timePastUTCString, timeNowUTCString}, "..")
	return timeResult, nil
}

func FirstStartAfterOff() {
	timeResult, err := GetTimeResult()
	if err != nil {
		log.Fatal(Wrap(err, "FirstStartAfterOff"))
	}
	data, err := GetData(timeResult)
	if err != nil {
		log.Fatal(Wrap(err, "FirstStartAfterOff"))
	}
	err = DbEntry(data)
	if err != nil {
		log.Fatal(Wrap(err, "FirstStartAfterOff"))
	}
}

func GetDataAndEntryInDb(ch chan int) {
	for {
		time.Sleep(waitTime)
		timeResult, err := GetTimeResult()
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		data, err := GetData(timeResult)
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		<- ch
		err = DbEntry(data)
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		ch <- 1
	}
}

func PrintStat(data []interface{}) {
	fmt.Printf("Статистика за последние 24 часа:\nКоличество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(BTC): %.8f\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f\nСредний размер блока(мбайты): %.2f\nКоличество входов во всех транзакциях всех блоков: %v\nКоличество выходов во всех транзакциях всех блоков: %v\nСумма входов во всех блоках(BTC): %.2f\nСумма выходов во всех блоках(BTC): %.2f\nСумма входов во всех блоках(USD): %.2f\nСумма выходов во всех блоках(USD): %.2f\nСуммарная награда майнеров за нахождение блоков(BTC): %.2f\nСуммарная награда майнеров за нахождение блоков(USD): %.2f\nСуммарная награда майнеров(за блок + комиссия)(BTC): %.2f\nСуммарная награда майнеров(за блок + комиссия)(USD): %.2f\n", data...)
}

func Menu(ch chan int) {
	for {
		fmt.Println("\nДля вывода статистики нажмите 1, для выхода нажмите 2.")
		var input int
		fmt.Scan(&input)
		switch input {
		case 1:
			timeNowUTC, timePastUTC := GetTime()
			<- ch
			timeNow := time.Now()
			data, err := DbStat(TimeToString(timeNowUTC), TimeToString(timePastUTC))
			if err != nil {
				log.Fatal(Wrap(err, "Menu"))
			}
			timeElapsed := time.Since(timeNow)
			ch <- 1
			fmt.Printf("Elapse time %v\n", timeElapsed)
			PrintStat(data)
		case 2:
			return
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}

func main() {
	FirstStartAfterOff()
	ch := make(chan int, 1)
	ch <- 1
	go GetDataAndEntryInDb(ch)
	Menu(ch)
}
