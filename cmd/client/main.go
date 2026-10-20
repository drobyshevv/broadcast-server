package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8082")
	if err != nil {
		println(err)
		return
	}
	go Read(conn)
	scannerWriter := bufio.NewScanner(os.Stdin)
	for scannerWriter.Scan() {
		s := scannerWriter.Text()
		_, err := conn.Write([]byte(s + "\n"))
		if err != nil {
			fmt.Println(err)
			break
		}
	}
}

func Read(conn net.Conn) {
	scannerReader := bufio.NewScanner(conn)
	for scannerReader.Scan() {
		fmt.Println(scannerReader.Text())
		if scannerReader.Err() != nil {
			fmt.Println(scannerReader.Err().Error())
			break
		}
	}
}
