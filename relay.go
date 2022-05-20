package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

type NodeData struct {
	NodeID  int    `json:"nodeid"`
	Data    string `json:"data"`
	Payload string `json:"payload"`
}

//------------------------------------------------------------------------------

var (
	Address = "0.0.0.0"
	Port    = "9000"

	NodeID = 0
	NodeEx = "cnterra-node-data"
	Key    = ""

	RbAddress  = "localhost"
	RbPort     = "5672"
	RbUser     = "guest"
	RbPassword = "guest"
)

var channel *amqp.Channel

//------------------------------------------------------------------------------

func initialize() {
	if str, found := os.LookupEnv("NODE_ID"); found {
		n, err := strconv.ParseInt(str, 10, 0)
		if err != nil {
			log.Fatalln("[ERRO] Invalid 'NODE_ID'")
		}
		NodeID = int(n)
		Key = fmt.Sprintf("node.%d.data.out", NodeID)
	} else {
		log.Fatalln("[ERRO] Variable 'NODE_ID' not set")
	}

	if str, found := os.LookupEnv("RELAY_ADDRESS"); found {
		Address = str
	}
	if str, found := os.LookupEnv("RELAY_PORT"); found {
		Port = str
	}

	if str, found := os.LookupEnv("RABBITMQ_ADDRESS"); found {
		RbAddress = str
	}
	if str, found := os.LookupEnv("RABBITMQ_PORT"); found {
		RbPort = str
	}
	if str, found := os.LookupEnv("RABBITMQ_USER"); found {
		RbUser = str
	}
	if str, found := os.LookupEnv("RABBITMQ_PASSORD"); found {
		RbPassword = str
	}
}

func relay(conn net.Conn) {
	q, err := channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Println("[ERRO] Error declaring a queue:", err)
		return
	}

	defer channel.QueueDelete(q.Name, false, false, true)

	err = channel.QueueBind(
		q.Name,              // queue name
		Key,                 // routing key
		"cnterra-node-data", // exchange
		false,
		nil)
	if err != nil {
		log.Println("[ERRO] Error binding a queue:", err)
		return
	}

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		fmt.Println("[ERRO] Error to register a consumer:", err)
		return
	}

	nodeData := NodeData{}
	for msg := range msgs {
		err := json.Unmarshal(msg.Body, &nodeData)
		if err != nil {
			fmt.Println("[ERRO] Error decoding the payload:", err)
			return
		}

		data, err := base64.StdEncoding.DecodeString(nodeData.Payload)
		if err != nil {
			fmt.Println("[ERRO] Error decoding base64:", err)
			return
		}

		n, err := conn.Write(data)
		if err != nil {
			fmt.Println("[ERRO] Error sending data:", err)
			return
		}
		if n != len(data) {
			fmt.Println("[WARN] Data sent:", n)
		}
	}
}

func listen() {
	addr := fmt.Sprintf("%s:%s", Address, Port)

	server, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[ERRO] Error listen on '%s': %s\n", addr, err)
	}

	for {
		log.Printf("[INFO] Server waiting connection on '%s'\n", addr)

		conn, err := server.Accept()
		if err != nil {
			log.Println("[ERRO] Failed to accept new connection:", err)
			continue
		}

		log.Println("[INFO] Serving a new connection")
		relay(conn)
		conn.Close()
	}
}

func main() {
	initialize()

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", RbUser, RbPassword, RbAddress, RbPort)

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalln("[ERRO] Error connecting to RabbitMQ:", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		log.Fatalln("[ERRO] Error openning a channel:", err)
	}

	err = channel.ExchangeDeclare(
		NodeEx,  // name
		"topic", // type
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatalln("[ERRO] Error declaring the controller exchange:", err)
	}

	listen()
}
