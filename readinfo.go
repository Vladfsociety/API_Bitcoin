package main

import (
	"io/ioutil"
	"net/http"
	"strconv"
	"github.com/tidwall/gjson"
)

const(
    stepOffset = 100
    attrCount = 18
  )

type Block struct {
  attribute []interface{}
}

func Check(err error) {
	if err != nil {
    panic(err)
  }
}

func GetSlice(gjson []gjson.Result) []Block {
  data := make([]Block, len(gjson[0].Array()))
  for block, _ := range gjson[0].Array() {
    data[block].attribute = make([]interface{}, attrCount)
    for attr, _ := range gjson {
      data[block].attribute[attr] = gjson[attr].Array()[block].Raw
    }
  }
  return data
}

func GetSliceResult(gjsonResult [][]gjson.Result) []Block {
  dataResult := make([]Block, 0)
  for index, _ := range gjsonResult {
    data := GetSlice(gjsonResult[index])
    dataResult = append(dataResult, data...)
  }
  return dataResult
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
  Check(err)
  json, err := ioutil.ReadAll(resp.Body)
  Check(err)
  if !gjson.ValidBytes(json) {
    panic("invalid json")
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

func GetData(timeResult string) []Block {
  json := GetJsonResult(timeResult)
  gjson := GetGjsonResult(json)
  return GetSliceResult(gjson)
}
