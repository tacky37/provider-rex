package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	sxmqtt "github.com/synerex/proto_mqtt"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	topic           = flag.String("topic", "cmd/app/b8ceb5de89b73fe09d915979bcf013a1/move-position", "MQTT Topic")
	mes             = flag.String("message", "", "Message")
	mFile           = flag.String("mfile", "", "message file")
	sxServerAddress string
)

func sendMQTTmessage(client *sxutil.SXServiceClient, tpc *string, msg *string) {
	rcd := sxmqtt.MQTTRecord{
		Topic:  *tpc,
		Time:   ptypes.TimestampNow(),
		Record: []byte(*msg),
	}

	out, _ := proto.Marshal(&rcd) // TODO: handle error
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "MQTT_Publish",
		Cdata: &cont,
	}
	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure: %v", nerr)
	}
}

func main() {
	log.Printf("MQTT_Pubilsher(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.MQTT_GATEWAY_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "MQTT-Publisher", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Clt:MQTT-Pub}")
	sclient := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON)

	if *mFile != "" {
		bytes, err := ioutil.ReadFile(*mFile)
		if err != nil {
			log.Fatal("Can't open file:", *mFile)
		} else {
			message := string(bytes)
			sendMQTTmessage(sclient, topic, &message)
		}

	} else {
		sendMQTTmessage(sclient, topic, mes)
	}

	sxutil.CallDeferFunctions() // cleanup!

}
