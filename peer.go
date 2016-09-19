package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/nu7hatch/gouuid"

	"code.cloudfoundry.org/lager"
)

type peerService struct {
	logger    lager.Logger
	members   *[]string
	heartbeat heartbeat
}

type heartbeat struct {
	GUID   string `json:"guid"`
	Secret string `json:"string"`
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (p peerService) peerListen(port int) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.IPv4(0, 0, 0, 0),
		Port: port,
	})

	if err != nil {
		p.logger.Fatal(err.Error(), err)
	}

	for {
		data := make([]byte, 256)
		read, remoteAddr, err := socket.ReadFromUDP(data)
		if err != nil {
			p.logger.Fatal(err.Error(), err)
		}

		phb := &heartbeat{}
		err = json.Unmarshal(data[:read], phb)

		// Add peer if not in slice
		if !stringInSlice(remoteAddr.String(), *p.members) && phb.Secret == p.heartbeat.Secret && phb.GUID != p.heartbeat.GUID {
			*p.members = append(*p.members, remoteAddr.String())

			p.logger.Info(fmt.Sprintf("Discovered new peer: %+v", remoteAddr))
		}

		p.logger.Debug(fmt.Sprintf("Addr: %+v", remoteAddr))
		p.logger.Debug(fmt.Sprintf("Heartbeat: %+v", phb))
	}
}

func (p peerService) peerBroadcast(port int) {
	broadcastIPv4 := net.IPv4(255, 255, 255, 255)
	socket, err := net.DialUDP("udp4", nil, &net.UDPAddr{
		IP:   broadcastIPv4,
		Port: port,
	})

	if err != nil {
		panic(err.Error())
	}

	for {
		buf, err := json.Marshal(p.heartbeat)
		_, err = socket.Write(buf)
		if err != nil {
			p.logger.Fatal(err.Error(), err)
		}
		time.Sleep(time.Second * 2)
	}
}

func (p peerService) writeValue() {

}

func (p peerService) statusHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	membersJSON, err := json.Marshal(p.members)
	p.logger.Info(fmt.Sprintf("%+v", p.members))
	if err != nil {
		p.logger.Fatal(err.Error(), err)
	}

	w.Write(membersJSON)
}

func main() {

	logger := lager.NewLogger("peer-service")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))

	peerGUID, _ := uuid.NewV4()
	secret := os.Args[1]

	heartbeat := heartbeat{GUID: peerGUID.String(), Secret: secret}

	s := peerService{logger: logger, heartbeat: heartbeat}
	var members []string
	s.members = &members
	http.HandleFunc("/", s.statusHandler)

	go s.peerListen(8888)
	go s.peerBroadcast(8888)

	err := http.ListenAndServe(":8080", nil)
	logger.Fatal(err.Error(), err)
}
