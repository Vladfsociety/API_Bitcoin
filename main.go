package main

import (
		//"fmt"
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

func main() {
	timeNow, timeDbLast := GetTime()
	timeResult := GetTimeString(timeNow, timeDbLast)
	dataSlice := GetDataDay(timeResult)
	DbEntry(dataSlice)
	DbStat(TimeToString(timeNow), TimeToString(timeNow.Add(-24 * time.Hour)))
}
