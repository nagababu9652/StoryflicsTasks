package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

type Proschema struct {
	Id     gocql.UUID `json:"Id"`
	Tags   []string   `json:"Tags"`
	Genres []string   `json:"Geners"`
}

func mainPro(jbyte []byte) {

	// connect to the cluster
	cluster := gocql.NewCluster("localhost:9042") //list of IP addresses used by your cluster.
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
		return
	}
	defer session.Close()

	// create keyspaces if not available
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	// create table if not available
	err = session.Query("CREATE TABLE IF NOT EXISTS test.userWatchedTags (userid uuid, tags map<text,int>, PRIMARY KEY (userid));").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	// unmarshal json and save in msg
	var msg = []byte(jbyte)
	m := Proschema{}
	json.Unmarshal(msg, &m)

	videoTags := append(m.Genres, m.Tags...)
	fmt.Println(videoTags)

	// if user exists get the tags of the user and save in keys
	var tags map[string]int
	result := session.Query("select tags from test.userwatchedTags where userid=?", m.Id).Iter()
	result.Scan(&tags)
	keys := make([]string, len(tags))
	i := 0
	for k := range tags {
		keys[i] = k
		i++
	}

	for i := 0; i < len(videoTags); i++ {
		//add new tag to db
		if !contains1(keys, videoTags[i]) {
			err = session.Query(`update test.userwatchedTags set tags[?]=1 where userid=?;`, videoTags[i], m.Id).Exec()
			if err != nil {
				log.Println("err: ", err)
			}

		} else {
			//increment old tag of db
			result := session.Query("select tags from test.userwatchedTags where userid=?", m.Id).Iter()
			result.Scan(&tags)
			err = session.Query(`update test.userwatchedTags set tags[?]=? where userid=?;`, videoTags[i], tags[videoTags[i]]+1, m.Id).Exec()
			if err != nil {
				log.Println("err: ", err)
			}

		}
	}

}

// function to check tag already in the key slice
func contains1(s []string, st string) bool {
	for _, v := range s {
		if v == st {
			return true
		}
	}
	return false
}
