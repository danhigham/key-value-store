package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/gorilla/mux"
	"github.com/nu7hatch/gouuid"

	"code.cloudfoundry.org/lager"
)

type peerService struct {
	logger    lager.Logger
	members   *[]string
	heartbeat heartbeat
	values    *map[string]string
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
	serverAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	socket, err := net.ListenUDP("udp", serverAddr)

	defer socket.Close()

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
		//
		if !stringInSlice(remoteAddr.String(), *p.members) && phb.Secret == p.heartbeat.Secret && phb.GUID != p.heartbeat.GUID {
			*p.members = append(*p.members, remoteAddr.String())

			p.logger.Info(fmt.Sprintf("Discovered new peer: %+v", remoteAddr))
		}

	}
}

func (p peerService) peerBroadcast(port int) {
	serverAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", port))
	localAddr, _ := net.ResolveUDPAddr("udp", ":0")
	socket, err := net.DialUDP("udp", localAddr, serverAddr)

	if err != nil {
		p.logger.Fatal(err.Error(), err)
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

func (p peerService) writeHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(405)
		return
	}

	vars := mux.Vars(req)
	key := vars["key"]

	buf, err := ioutil.ReadAll(req.Body)

	if err != nil {
		p.logger.Fatal(err.Error(), err)
	}

	members := (*p.members)
	r, _ := regexp.Compile("([0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}):")

	for _, peer := range members {
		url := fmt.Sprintf("http://%s:8080/peer-write/%s/", r.FindStringSubmatch(peer)[1], key)
		p.logger.Info(fmt.Sprintf("Posting '%s' to '%s'", key, url))
		http.Post(url, "text/plain", bytes.NewReader(buf))
	}

	// write to peers
	(*p.values)[key] = string(buf)
	p.logger.Info(fmt.Sprintf("Wrote value '%s' for key '%s'", (*p.values)[key], key))
	w.WriteHeader(201)
}

func (p peerService) readHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		w.WriteHeader(405)
		return
	}

	vars := mux.Vars(req)
	key := vars["key"]

	value := (*p.values)[key]

	w.WriteHeader(201)
	w.Header().Set("Content-Type", "plain/text")
	w.Write([]byte(value))
}

func (p peerService) peerWriteHandler(w http.ResponseWriter, req *http.Request) {

	// check to see if the request is coming from a node in members

	vars := mux.Vars(req)
	key := vars["key"]

	buf, err := ioutil.ReadAll(req.Body)

	if err != nil {
		p.logger.Fatal(err.Error(), err)
	}

	(*p.values)[key] = string(buf)
	w.WriteHeader(201)
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
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))

	peerGUID, _ := uuid.NewV4()
	secret := os.Args[1]

	heartbeat := heartbeat{GUID: peerGUID.String(), Secret: secret}

	s := peerService{logger: logger, heartbeat: heartbeat}
	var members []string
	s.members = &members

	values := make(map[string]string)
	s.values = &values

	r := mux.NewRouter()
	r.HandleFunc("/", s.statusHandler)
	r.HandleFunc("/write/{key}/", s.writeHandler)
	r.HandleFunc("/peer-write/{key}/", s.peerWriteHandler)
	r.HandleFunc("/read/{key}/", s.readHandler)
	http.Handle("/", r)

	go s.peerListen(1234)
	go s.peerBroadcast(1234)

	err := http.ListenAndServe(":8080", nil)
	logger.Fatal(err.Error(), err)
}
