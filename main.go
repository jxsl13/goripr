package main

import (
	"fmt"
	"os"
	"os/signal"
)

func main() {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer close(interrupt)

	rdb, err := NewRedisClient(RedisOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err != nil {
		fmt.Printf("error NewRedisClient: %v", err)
		return
	}
	defer rdb.Close()

	err = rdb.InsertRangeUnsafe("122.0.0.1 - 123.0.0.1", "test reason")
	if err != nil {
		fmt.Println(err)
		return
	}

	err = rdb.InsertRangeUnsafe("127.0.0.1 - 128.0.0.1", "test reason")
	if err != nil {
		fmt.Println(err)
		return
	}

	insideRange := "122.0.0.1 - 128.0.0.1"

	inside, err := rdb.Inside(insideRange)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Inside: ", insideRange)
		for _, attr := range inside {
			fmt.Println(attr)
		}
	}

	insideRange = "123.0.0.1"

	inside, err = rdb.Inside(insideRange)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Inside: ", insideRange)
		for _, attr := range inside {
			fmt.Println(attr)
		}
	}

	testIP := "121.0.0.1"

	ip, err := rdb.Below(testIP)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("below:", ip)
		intIP, _ := IPToInt(ip.IP)
		fmt.Println("IP Integer representation: ", intIP)
	}

	ip, err = rdb.Above(testIP)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("above:", ip)
		intIP, _ := IPToInt(ip.IP)
		fmt.Println("IP Integer representation: ", intIP)
	}

	fmt.Print("Finished successfully!\n\n\n")

	below, above, err := rdb.Neighbours(testIP, 2)

	if err != nil {
		fmt.Println(err)
	} else {
		for _, ip := range below {
			fmt.Println("below:", ip)
			intIP, _ := IPToInt(ip.IP)
			fmt.Println("IP as int:", intIP)
		}

		for _, ip := range above {
			fmt.Println("above:", ip)
			intIP, _ := IPToInt(ip.IP)
			fmt.Println("IP as int:", intIP)
		}
	}

	<-interrupt

	_, err = rdb.FlushAll().Result()
	if err != nil {
		fmt.Println("error flushing database: %w", err)
	} else {
		fmt.Println("Successfully flushed database.")
	}
}
