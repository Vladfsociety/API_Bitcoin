package main

import (
		"fmt"
		"time"
		"strings"
)

func StringToTime(timeString string) time.Time {
	timeString = timeString[0:len(timeString)-1]
  timeString = strings.Join(strings.Split(timeString, "T"), " ")
	time, err := time.Parse("2006-01-02 15:04:05", timeString)
	Check(err)
	return time
}

func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05")
}

func LaterTime(dbLastTime, timePast time.Time) time.Time {
	if dbLastTime.After(timePast) {
		return dbLastTime.Add(1 * time.Second)
	}
	return timePast
}

func GetTime() (time.Time, time.Time) {
	timeNow := time.Now()
	timeNow = timeNow.Add(-3 * time.Hour)
	var timeDbLast time.Time
	var err error
	if DbEmpty() {
    timeDbLast, err = time.Parse("2006-01-02 15:04:05", "2001-01-01 12:00:00")
		Check(err)
  } else {
		timeDbLast = DbLastTime()
	}
	return timeNow, timeDbLast
}

func GetTimeString(timeNow, timeDbLast time.Time) string {
	timePast := LaterTime(timeDbLast, timeNow.Add(-24 * time.Hour))
	timeNowString := TimeToString(timeNow)
	timePastString := TimeToString(timePast)
  timeResult := strings.Join([]string{timePastString, timeNowString}, "..")
	return timeResult
}

func GetDataAndEntryInDb(ch1 chan int) {
	for {
		timeNow, timeDbLast := GetTime()
		timeResult := GetTimeString(timeNow, timeDbLast)
		dataSlice := GetDataDay(timeResult)
		<- ch1
		DbEntry(dataSlice)
		ch1 <- 1
		time.Sleep(time.Second * 10)
	}
}

func Sync(ch1 chan int) {
	for {
		ch1 <- 1
		<- ch1
	}
}

func main() {
	ch := make(chan int, 1)
	var input int
	fmt.Println("Для выхода нажмите 1, для вывода статистики нажмите 2.")
	go Sync(ch)
	go GetDataAndEntryInDb(ch)
	for {
		fmt.Scan(&input)
		switch input {
		case 1:
			return
		case 2:
			<- ch
			timeNow, _ := GetTime()
			DbStat(TimeToString(timeNow), TimeToString(timeNow.Add(-24 * time.Hour)))
			ch <- 1
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}
