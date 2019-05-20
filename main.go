package main

import (
		"fmt"
		"time"
		"strings"
)

func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05")
}

func LaterTime(dbLastTime, timePastUTC time.Time) time.Time {
	if dbLastTime.After(timePastUTC) {
		return dbLastTime.Add(1 * time.Second)
	}
	return timePastUTC
}

func GetTime() (time.Time, time.Time) {
	timeNow := time.Now()
	timeNowUTC := timeNow.Add(-3 * time.Hour)
	return timeNowUTC, timeNowUTC.Add(-24 * time.Hour)
}

func GetTimeResult(timeNowUTC, timePastUTC time.Time) string {
	if !DbEmpty() {
		timePastUTC = LaterTime(DbLastTime(), timePastUTC)
  }
	timeNowUTCString := TimeToString(timeNowUTC)
	timePastUTCString := TimeToString(timePastUTC)
  timeResult := strings.Join([]string{timePastUTCString, timeNowUTCString}, "..")
	return timeResult
}

func FirstStartAfterOff() {
	timeNowUTC, timePastUTC := GetTime()
	timeResult := GetTimeResult(timeNowUTC, timePastUTC)
	dataSlice := GetData(timeResult)
	DbEntry(dataSlice)
}

func GetDataAndEntryInDb(ch chan int) {
	for {
		time.Sleep(time.Second * 20)
		timeNowUTC, timePastUTC := GetTime()
		timeResult := GetTimeResult(timeNowUTC, timePastUTC)
		dataSlice := GetData(timeResult)
		<- ch
		DbEntry(dataSlice)
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
			<- ch
			timeNowUTC, timePastUTC := GetTime()
			DbStat(TimeToString(timeNowUTC), TimeToString(timePastUTC))
			ch <- 1
		case 2:
			return
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}
