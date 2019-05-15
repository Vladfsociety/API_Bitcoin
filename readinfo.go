package main

import (
	"fmt"
	"log"
	"time"
	"strings"
	"io/ioutil"
	"net/http"
	"strconv"
	"github.com/tidwall/gjson"
)

const(
    stepOffset = 100
    attrQuantity = 18
  )

type Block struct {
  attribute []interface{}
}

func GetSlice(gjson []gjson.Result) []Block {
  dataSlice := make([]Block, len(gjson[0].Array()))
  for block, _ := range gjson[0].Array() {
    dataSlice[block].attribute = make([]interface{}, attrQuantity)
    for attr, _ := range gjson {
      dataSlice[block].attribute[attr] = gjson[attr].Array()[block].Raw
    }
  }
  return dataSlice
}

func GetSliceResult(gjsonResult [][]gjson.Result) []Block {
  dataSliceResult := make([]Block, 0)
  for i, _ := range gjsonResult {
    dataSlice := GetSlice(gjsonResult[i])
    dataSliceResult = append(dataSliceResult, dataSlice...)
  }
  return dataSliceResult
}

func GetGjsonResult(jsonResult [][]byte) [][]gjson.Result {
  gjsonResult := make([][]gjson.Result, len(jsonResult))
  for i, _ := range jsonResult {
    gjson := gjson.GetManyBytes(jsonResult[i], "data.#.id", "data.#.time", "data.#.median_time", "data.#.size", "data.#.difficulty", "data.#.transaction_count", "data.#.input_count", "data.#.output_count", "data.#.input_total", "data.#.input_total_usd", "data.#.output_total", "data.#.output_total_usd", "data.#.fee_total", "data.#.fee_total_usd", "data.#.generation", "data.#.generation_usd", "data.#.reward", "data.#.reward_usd")
    gjsonResult[i] = gjson
  }
  return gjsonResult
}

func Empty(json []byte) bool {
  gjson := gjson.GetManyBytes(json, "data.#.id")
  if len(gjson[0].Array()) == 0 {
    return true
  }
  return false
}

func GetJson(timeResult string, offset int) []byte {
  offsetString := strconv.Itoa(offset)
  resp, err := http.Get("https://api.blockchair.com/bitcoin/blocks?q=time(" + timeResult + ")&s=time(desc)&limit=100&offset=" + offsetString)
  if err != nil {
    log.Fatalln(err)
  }
  json, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Fatalln(err)
  }
  if !gjson.Valid(string(json)) {
    fmt.Println("invalid json")
  }
  return json
}

func GetJsonResult(timeResult string) [][]byte {
  jsonResult := make([][]byte, 0)
  offset := 0
  for {
    json := GetJson(timeResult, offset)
    if Empty(json) {
      break
    }
    jsonResult = append(jsonResult, json)
    offset += stepOffset
  }
  return jsonResult
}

func GetTime() string {
	timeNow := time.Now()
	timeNow = timeNow.Add(-3 * time.Hour)
	timePast := timeNow.Add(-24 * time.Hour)
	timeNowString := timeNow.Format(time.RFC3339)[:19]
	timePastString := timePast.Format(time.RFC3339)[:19]
	timeNowString = strings.Join(strings.Split(timeNowString, "T"), " ")
	timePastString = strings.Join(strings.Split(timePastString, "T"), " ")
  timeResult := strings.Join([]string{timePastString, timeNowString}, "..")
	return timeResult
}

func GetData24H() []Block {
	timeResult := GetTime()
	fmt.Println(timeResult)
  json := GetJsonResult(timeResult)
  gjson := GetGjsonResult(json)
  return GetSliceResult(gjson)
}
