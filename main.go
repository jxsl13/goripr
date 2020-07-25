package main

import (
	"fmt"
)

func main() {

	//interrupt := make(chan os.Signal, 1)
	//signal.Notify(interrupt, os.Interrupt)
	//defer close(interrupt)

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

	// err = rdb.InsertRangeUnsafe("122.0.0.1 - 123.0.0.1", "test reason")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	ip, err := rdb.IPBelow("123.0.0.0")

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(ip)

	intIP, _ := IPToInt(ip.IP)

	fmt.Println("IP Integer representation: ", intIP)

	// err := insertRange(rdb, "123.0.0.0/24", "VPN")
	// if err != nil {
	// 	log.Println("error: ", err)
	// }

	// zm, err := aboveIP(rdb, "123.0.0.20")
	// if err != nil {
	// 	log.Println(err)
	// } else {
	// 	fmt.Printf("Above IP: %s ID: %s", zm.IP.String(), zm.ID)
	// }
	fmt.Println("Finished successfully!")

	//<-interrupt

	_, err = rdb.FlushAll().Result()
	if err != nil {
		fmt.Println("error flushing database: %w", err)
	} else {
		fmt.Println("Successfully flushed database.")
	}
}
