package main

import (
		"log"
		"fmt"
		"time"
		"strings"
)

const(
	deltaUTC = 3 * time.Hour // Разница между нашим временем и UTC.
	oneDay = 24 * time.Hour // Один день.
	waitTime = 20 * time.Second // Время между обновлением бд.
	deltaDb = 1 * time.Second // Небольшая добавка к времени нахождения последнего блока в бд, чтобы в дальнейшем предотвратить запись  уже находящегося там блока.
)

// Преобразование времени в строку
func TimeToString(time time.Time) string {
	return time.Format("2006-01-02 15:04:05")
}

// Функция нахождения текущего времени в UTC.
func TimeNowUTC() time.Time {
	timeNow := time.Now()
	return timeNow.Add(-deltaUTC)
}

// Функция для получения времени нахождения последнего блока в нашей бд, или если бд пуста, то настоящего времени в UTC минус 24 часа.
func TimePastUTC(timeNowUTC time.Time) (time.Time, error) {
	timePastUTC := timeNowUTC.Add(-oneDay)
	empty, err := DbEmpty()
	if err != nil {
		return timePastUTC, Wrap(err, "GetValidTimePast")
	}
	if !empty {
		dbLastTime, err := DbLastTime()
		if err != nil {
			return timePastUTC, Wrap(err, "GetValidTimePast")
		}
		return dbLastTime.Add(deltaDb), nil
  }
	return timePastUTC, nil
}

// Функция для получения строки со временем в том виде, какой необходимо будет испольовать при запросе к стороннему АПИ.
func TimeResult() (string, error) {
	timeNowUTC := TimeNowUTC()
	timePastUTC, err := TimePastUTC(timeNowUTC)
	if err != nil {
		return "", Wrap(err, "GetTimeResult")
	}
	timeNowUTCString := TimeToString(timeNowUTC)
	timePastUTCString := TimeToString(timePastUTC)
  timeResult := strings.Join([]string{timePastUTCString, timeNowUTCString}, "..")
	return timeResult, nil
}

// С некоторой периодичностью получает информацию по блокам от стороннего АПИ и записывает в базу данных.
func GetDataAndEntryInDb(ch chan int) {
	for {
		<- ch
		timeResult, err := TimeResult()
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		ch <- 1
		data, err := GetData(timeResult)
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		<- ch
		err = DbEntry(data)
		if err != nil {
			log.Fatal(Wrap(err, "GetDataAndEntryInDb"))
		}
		ch <- 1
		time.Sleep(waitTime)
	}
}

// Выводит в консоль статистику, записанную в data.
func PrintStat(data []interface{}) {
	fmt.Printf("Статистика за последние 24 часа:\nКоличество блоков: %v\nКоличество транзакций: %v\nСредняя комиссия за транзакцию(BTC): %.8f\nСредняя комиссия за транзакцию(USD): %.2f\nСреднее время между блоками(секунды): %.2f\nСредний размер блока(мбайты): %.2f\nКоличество входов во всех транзакциях всех блоков: %v\nКоличество выходов во всех транзакциях всех блоков: %v\nСумма входов во всех блоках(BTC): %.2f\nСумма выходов во всех блоках(BTC): %.2f\nСумма входов во всех блоках(USD): %.2f\nСумма выходов во всех блоках(USD): %.2f\nСуммарная награда майнеров за нахождение блоков(BTC): %.2f\nСуммарная награда майнеров за нахождение блоков(USD): %.2f\nСуммарная награда майнеров(за блок + комиссия)(BTC): %.2f\nСуммарная награда майнеров(за блок + комиссия)(USD): %.2f\n", data...)
}

// Функция ждет ввод от пользователя и в зависимости от результата может: предоставить статистику по биткоину, завершить программу или сообщить о неправильном вводе.
func Menu(ch chan int) {
	for {
		fmt.Println("\nДля вывода статистики нажмите 1, для выхода нажмите 2.")
		var input int
		fmt.Scan(&input)
		switch input {
		case 1:
			timeNowUTC := TimeNowUTC()
			<- ch
			data, err := DbStat(TimeToString(timeNowUTC), TimeToString(timeNowUTC.Add(-oneDay)))
			if err != nil {
				log.Fatal(Wrap(err, "Menu"))
			}
			ch <- 1
			PrintStat(data)
		case 2:
			return
		default:
			fmt.Println("Хорошая попытка, но нет.")
		}
	}
}

func main() {
	ch := make(chan int, 1) // Канал для того, чтобы в каждый момент времени доступ к базе данных был только у одной функции GetDataAndEntryInDb или Menu.
	ch <- 1
	go GetDataAndEntryInDb(ch)
	time.Sleep(2 * time.Second)
	Menu(ch)
}
