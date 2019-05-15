package main

import (
	//"fmt"
)
/*
func requestHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello, world")
}
*/
func main(){
	dataSlice := GetData24H()
	DatabaseEntry(dataSlice)
}
