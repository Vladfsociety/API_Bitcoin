package main

import (
		//"fmt"
		"time"
		"strings"
)
/*
func requestHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello, world")
}
*/
func EarlierTime(dbLastTime, timePastTime time.Time) time.Time {
	if dbLastTime.After(timePastTime) {
		return dbLastTime.Add(1 * time.Second)
	}
	return timePastTime
}

func GetTime(dbLastTime time.Time) string {
	timeNowTime := time.Now()
	timeNowTime = timeNowTime.Add(-3 * time.Hour)
	timePastTime := timeNowTime.Add(-24 * time.Hour)
	timePastTime = EarlierTime(dbLastTime, timePastTime)
	timeNowString := timeNowTime.Format("2006-01-02 15:04:05")
	timePastString := timePastTime.Format("2006-01-02 15:04:05")
  timeResult := strings.Join([]string{timePastString, timeNowString}, "..")
	return timeResult
}

func main() {
	/*var dbLastTime time.Time
  if DbEmpty() {
		var err error
    dbLastTime, err = time.Parse("2006-01-02 15:04:05", "2001-01-01 12:00:00")
		Check(err)
  } else {
		dbLastTime = DbLastTime()
	}*/
	timeResult := GetTime(DbLastTime())
	dataSlice := GetDataDay(timeResult)
	DbEntry(dataSlice)
	DbStat()
}
