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
  attributes []interface{}
}

func GetSlice(gjson []gjson.Result) []Block {
  data := make([]Block, len(gjson[0].Array()))
  for block, _ := range gjson[0].Array() {
    data[block].attributes = make([]interface{}, attrCount)
    for attr, _ := range gjson {
      data[block].attributes[attr] = gjson[attr].Array()[block].Raw
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

func GetJson(timeResult string, offset int) ([]byte, error) {
  offsetString := strconv.Itoa(offset)
	query := "https://api.blockchair.com/bitcoin/blocks?q=time(" + timeResult + ")&s=time(desc)&limit=100&offset=" + offsetString
  resp, err := http.Get(query)
	if err != nil {
	  return make([]byte, 0), Wrapf(err, "GetJson: Ошибка при запросе к %s", query)
	}
	defer resp.Body.Close()
	json, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return json, Wrap(err, "GetJson: Ошибка при считывании тела response в []byte")
	}
  if !gjson.ValidBytes(json) {
    return json, NewErr("GetJson: Получен неправильный json")
  }
  return json, nil
}

func GetJsonResult(timeResult string) ([][]byte, error) {
  jsonResult := make([][]byte, 0)
  offset := 0
  for {
    json, err := GetJson(timeResult, offset)
		if err != nil {
			return jsonResult, Wrap(err, "GetJsonResult")
		}
    if Empty(json) {
      break
    }
    jsonResult = append(jsonResult, json)
    offset += stepOffset
  }
  return jsonResult, nil
}

func GetData(timeResult string) ([]Block, error) {
  json, err := GetJsonResult(timeResult)
	if err != nil {
		return make([]Block, 0), Wrap(err, "GetData")
	}
  gjson := GetGjsonResult(json)
  return GetSliceResult(gjson), nil
}

/*2019/05/23 15:05:58 GetDataAndEntryInDb: GetData: GetJsonResult: GetJson: Ошибка при запросе к https://api.blockchair.com/bitcoin/blocks?q=time(2019-05-23 11:57:09..2019-05-23 12:05:38)&s=time(desc)&limit=100&offset=0: Get https://api.blockchair.com/bitcoin/blocks?q=time(2019-05-23 11:57:09..2019-05-23 12:05:38)&s=time(desc)&limit=100&offset=0: read tcp 192.168.0.101:63156->194.67.203.85:443: wsarecv: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond.*/
