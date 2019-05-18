package main

import (
		//"fmt"
		//"time"
)
/*
func requestHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello, world")
}
*/
func main() {
	//DatabaseClear()
	dataSlice := GetDataDay()
	DbEntry(dataSlice)
	DbStat()
}
