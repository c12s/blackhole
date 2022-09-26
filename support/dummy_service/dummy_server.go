package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

func handleRequestSuccess(writer http.ResponseWriter, req *http.Request) {
	handleRequest(writer, req, 200)
}
func handleRequestFailure(writer http.ResponseWriter, req *http.Request) {
	handleRequest(writer, req, 500)
}

func handleRequest(writer http.ResponseWriter, req *http.Request, returnStatusCode int) {
	id := mux.Vars(req)["id"]
	executionTime, err := strconv.Atoi(mux.Vars(req)["executionTime"])
	if err != nil {
		panic("Cant convert 'executionTime' path variable to int")
	}

	log.Printf("[Handler] Processing req with id %s", id)
	time.Sleep(time.Duration(executionTime) * time.Millisecond)
	log.Printf("[Handler] Returning status %d for req with id %s", returnStatusCode, id)
	writer.WriteHeader(returnStatusCode)
}
func startServer() {
	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/success/{id}/{executionTime}", handleRequestSuccess).Methods("GET")
	router.HandleFunc("/failure/{id}/{executionTime}", handleRequestFailure).Methods("GET")

	router.PathPrefix("/").Handler(http.FileServer(http.Dir("./static")))
	println("Server starting")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	startServer()
}
