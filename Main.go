package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

type Searchschema struct {
	Id     gocql.UUID `json:"Id"`
	Tags   []string   `json:"Tags"`
	Genres []string   `json:"Geners"`
}

func HomeLink(w http.ResponseWriter, r *http.Request) {

	var topGeners [10]string
	var videoidlist []gocql.UUID

	var getuserid, err = gocql.ParseUUID("028cf408-8c98-48d7-8d06-6efbae675d06")
	if err != nil {
		log.Println(err)
		fmt.Println("show most viewed videos in the user recommendation section")
	} else {
		topGeners = mainCon(getuserid)
		if topGeners[0] != "" {
			videoidlist = VideoGenerator(topGeners, getuserid)

			if len(videoidlist) > 0 {
				for _, num := range videoidlist {
					fmt.Fprintln(w, "---------------------------------------------------------")
					fmt.Fprintln(w, "     video ", num)
					fmt.Fprintln(w, "---------------------------------------------------------")
				}
			} else {
				fmt.Println("show most viewed videos in the user recommendation section")
			}

		}
	}

}

func ClickEvent(w http.ResponseWriter, r *http.Request) {
	var newEvent Proschema
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Kindly enter data with the event title and description only in order to update")
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)

	//Push data in to producer
	click, _ := json.Marshal(newEvent)
	mainPro(click)

}

func Videosuggest(w http.ResponseWriter, r *http.Request) {
	var newEvent Videoschema
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)

	//Push data in to producer
	click, _ := json.Marshal(newEvent)
	suggestionlist := Videosuggestion(click)

	for _, num := range suggestionlist {
		fmt.Fprintln(w, "---------------------------------------------------------")
		fmt.Fprintln(w, "     video ", num)
		fmt.Fprintln(w, "---------------------------------------------------------")
	}

}

func VidCompltEvent(w http.ResponseWriter, r *http.Request) {
	var newEvent Proschema1
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Kindly enter data with the event title and description only in order to update")
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)

	//Push data in to producer
	click, _ := json.Marshal(newEvent)
	completedvideos(click)

}
func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", HomeLink)
	router.HandleFunc("/click", ClickEvent).Methods("POST")
	router.HandleFunc("/videosuggest", Videosuggest).Methods("POST")
	router.HandleFunc("/videocompleted", VidCompltEvent).Methods("POST")
	log.Fatal(http.ListenAndServe(":8080", router))
}
