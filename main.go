package main

import (
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

func GetTimeResult(timeNowUTC, timePastUTC time.Time) string {
	if !DbEmpty() {
		timePastUTC = DbLastTime().Add(deltaDb)
  }
	timeNowUTCString := TimeToString(timeNowUTC)
	timePastUTCString := TimeToString(timePastUTC)
  timeResult := strings.Join([]string{timePastUTCString, timeNowUTCString}, "..")
	return timeResult
}

func FirstStartAfterOff() {
	timeNowUTC, timePastUTC := GetTime()
	timeResult := GetTimeResult(timeNowUTC, timePastUTC)
	data := GetData(timeResult)
	DbEntry(data)
}

func GetDataAndEntryInDb(ch chan int) {
	for {
		time.Sleep(waitTime)
		timeNowUTC, timePastUTC := GetTime()
		timeResult := GetTimeResult(timeNowUTC, timePastUTC)
		data := GetData(timeResult)
		<- ch
		DbEntry(data)
		ch <- 1
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
			data := DbStat(TimeToString(timeNowUTC), TimeToString(timePastUTC))
			ch <- 1
			fmt.Printf("Статистика за последниее 24 часа:\nКоличество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(BTC): %.8f\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f\nСредний размер блока(мбайты): %.2f\nКоличество входов во всех транзакциях блоков: %v\nКоличество выходов во всех транзакциях блоков: %v\nСумма входов во всех блоках(BTC): %.2f\nСумма выходов во всех блоках(BTC): %.2f\nСумма входов во всех блоках(USD): %.2f\nСумма выходов во всех блоках(USD): %.2f\nСуммарная награда майнеров за нахождение блоков(BTC): %.2f\nСуммарная награда майнеров за нахождение блоков(USD): %.2f\nСуммарная награда майнеров(за блок + комиссия)(BTC): %.2f\nСуммарная награда майнеров(за блок + комиссия)(USD): %.2f\n", data...)
		case 2:
			return
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}
