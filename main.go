package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Rule struct {
	listenTopic  string
	jsonSelector string
	jsonValue    string
	publishTopic string
	publishValue string
}

var rules = make([]Rule, 0)

func parseRules() {
	f, err := os.Open("mqtt.rules")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open rules file: %s", err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)
		if len(words) != 5 {
			fmt.Fprintf(os.Stderr, "unreognized line: %s\n", line)
			continue
		}
		rule := Rule{listenTopic: words[0], jsonSelector: words[1], jsonValue: words[2], publishTopic: words[3], publishValue: words[4]}
		rules = append(rules, rule)
	}
}

func getString(m map[string]interface{}, property string) (string, error) {
	val, ok := m[property]
	if !ok {
		return "", fmt.Errorf("property not found")
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("property not found")
	}
	return s, nil
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("%s: topic %s, data %s, qos %d\n", time.Now().Format("2006-01-02 15:04:05"), msg.Topic(), msg.Payload(), msg.Qos())

	processed := false
	for _, rule := range rules {
		if msg.Topic() != rule.listenTopic {
			continue
		}
		processed = true
		var obj interface{}
		json.Unmarshal(msg.Payload(), &obj)
		switch v := obj.(type) {
		case map[string]interface{}:
			val, _ := getString(v, rule.jsonSelector[1:])
			if val == rule.jsonValue {
				fmt.Printf("%s: publish %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), rule.publishTopic, rule.publishValue)
				client.Publish(rule.publishTopic, 0, false, rule.publishValue)
			}
			break
		default:
			fmt.Printf("unknown payload\n")
		}
	}
	if !processed {
		fmt.Fprintf(os.Stderr, "no rule matched\n")
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection Lost: %s\n", err.Error())
}

func main() {
	parseRules()

	if len(rules) == 0 {
		fmt.Fprintf(os.Stderr, "no rules set up\n")
		return
	}

	var broker = "tcp://raspberrypi.home:1883"
	options := mqtt.NewClientOptions()
	options.AddBroker(broker)
	options.SetClientID("mqttrulehandler")
	options.SetDefaultPublishHandler(messagePubHandler)
	options.OnConnect = connectHandler
	options.OnConnectionLost = connectionLostHandler

	client := mqtt.NewClient(options)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	for _, rule := range rules {
		topic := rule.listenTopic
		token = client.Subscribe(topic, 1, nil)
		token.Wait()
		fmt.Printf("Subscribed to topic %s\n", topic)
	}

	for {
		time.Sleep(time.Second)
	}

	//client.Disconnect(100)
}
