package main

import (
	"io/ioutil"
	"net/http"
	"strconv"
	"github.com/tidwall/gjson"
)

const (
    stepOffset = 100 // Шаг оффсета, напрямую зависит от того, что лимит в запросе к АПИ стоит в 100 строк.
    attrCount = 18 // Количество различных параметров блока
  )

// Содержит информацию об одном блоке.
type Block struct {
  attributes []interface{} // Какой либо параметр(id, time, size, difficulty, transaction_count...)
}

// Возвращает слайс блоков, с необходимой информацией. gjson[attr].Result представляет собой структуру в поле Raw которой содержится информация о атрибуте attr(id, time, size...) какого-то количества блоков, метод Array() преобразует структуру Result в []Resultов, где уже в одном Result записана информация о атрибуте одного блока.
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

// Преобразует gjson.Result в слайс блоков.
func GetSliceResult(gjsonResult [][]gjson.Result) []Block {
  dataResult := make([]Block, 0)
  for index, _ := range gjsonResult {
    data := GetSlice(gjsonResult[index])
    dataResult = append(dataResult, data...)
  }
  return dataResult
}

// Считывает необходимые поля из полученных jsonов в слайс gjson.Resultов.
func GetGjsonResult(jsonResult [][]byte) [][]gjson.Result {
  gjsonResult := make([][]gjson.Result, len(jsonResult))
  for i, _ := range jsonResult {
    gjson := gjson.GetManyBytes(jsonResult[i], "data.#.id", "data.#.time", "data.#.median_time", "data.#.size", "data.#.difficulty", "data.#.transaction_count", "data.#.input_count", "data.#.output_count", "data.#.input_total", "data.#.input_total_usd", "data.#.output_total", "data.#.output_total_usd", "data.#.fee_total", "data.#.fee_total_usd", "data.#.generation", "data.#.generation_usd", "data.#.reward", "data.#.reward_usd")
    gjsonResult[i] = gjson
  }
  return gjsonResult
}

// Проверяет пуст ли полученный json
func Empty(json []byte) bool {
  gjson := gjson.GetManyBytes(json, "data.#.id")
  if len(gjson[0].Array()) == 0 {
    return true
  }
  return false
}

// Отправляет Get-запрос к стороннему АПИ и возвращает ответ.
func QueryAPI(timeResult string, offset int) (*http.Response, error) {
	offsetString := strconv.Itoa(offset)
	query := "https://api.blockchair.com/bitcoin/blocks?q=time(" + timeResult + ")&s=time(desc)&limit=100&offset=" + offsetString
  resp, err := http.Get(query)
	if err != nil {
	  return resp, Wrapf(err, "QueryAPI: Ошибка при запросе к %s", query)
	}
	return resp, nil
}

// Преобразует тело ответа в слпйс байтов.
func RespToByte(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	json, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return json, Wrap(err, "RespToByte: Ошибка при считывании тела response в []byte")
	}
	return json, nil
}

// Функция отправляет запрос, получает ответ, преобразует в []byte и проверяет полученный файл на соответствие jsonу.
func GetJson(timeResult string, offset int) ([]byte, error) {
	resp, err := QueryAPI(timeResult, offset)
	if err != nil {
    return make([]byte, 0), Wrap(err, "GetJson")
  }
	json, err := RespToByte(resp)
	if err != nil {
    return json, Wrap(err, "GetJson")
  }
  if !gjson.ValidBytes(json) {
    return json, NewErr("GetJson: Получен неправильный json")
  }
  return json, nil
}

// Функция отправляющая запросы на получение данных. Стороннее АПИ может за раз выслать максимум 100 блоков в одном jsonе, чего не всегда может хватать, если база давно не обновлялась, поэтому в цикле с помощью offset получаем несколько jsonов и преобразуем в [][]byte.
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

// Функция отправляет запрос к стороннему АПИ, получает json, преобразует его в тип gjson.Result, из которого затем считаем данные в слайс, который и возвращается.
func GetData(timeResult string) ([]Block, error) {
  json, err := GetJsonResult(timeResult)
	if err != nil {
		return make([]Block, 0), Wrap(err, "GetData")
	}
  gjson := GetGjsonResult(json)
  return GetSliceResult(gjson), nil
}

/*2019/05/23 15:05:58 GetDataAndEntryInDb: GetData: GetJsonResult: GetJson: Ошибка при запросе к https://api.blockchair.com/bitcoin/blocks?q=time(2019-05-23 11:57:09..2019-05-23 12:05:38)&s=time(desc)&limit=100&offset=0: Get https://api.blockchair.com/bitcoin/blocks?q=time(2019-05-23 11:57:09..2019-05-23 12:05:38)&s=time(desc)&limit=100&offset=0: read tcp 192.168.0.101:63156->194.67.203.85:443: wsarecv: A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond.*/
