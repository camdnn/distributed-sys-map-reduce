package main

import (
	"fmt"
	"log"
	"os"
)

const M int = 9
const R int = 3
const filename = "../treasure_island.txt"

func main() {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("os.OpenFile", err)
	}
	defer file.Close()

	Coordinator(M, R, file)

	fmt.Println("Successfully ran file")

}
