package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gocql/gocql"
)

type Proschema1 struct {
	VideoId gocql.UUID `json:"VideoId"`
	UserId  gocql.UUID `json:"UserId"`
}

func completedvideos(jbyte []byte) {

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
	err = session.Query("CREATE TABLE IF NOT EXISTS test.completedvideo (userid uuid, videoids list<uuid>, primary key(userid));").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	// unmarshal json and save in msg
	var msg = []byte(jbyte)
	m := Proschema1{}
	json.Unmarshal(msg, &m)

	// validate user and video uuid types
	userstr := gocql.UUID.String(m.UserId)
	var getuserid, err1 = gocql.ParseUUID(userstr)
	if err1 != nil {
		log.Println(err)
		return
	}

	videoidstr := gocql.UUID.String(m.VideoId)
	var getvidoid, err2 = gocql.ParseUUID(videoidstr)
	if err2 != nil {
		log.Println(err)
		return
	}

	var add [1]gocql.UUID
	add[0] = m.VideoId
	invalidUUID, _ := gocql.ParseUUID("00000000-0000-0000-0000-000000000000")
	if getuserid != invalidUUID && getvidoid != invalidUUID {
		var compltVid []gocql.UUID
		iter2 := session.Query(`select videoids from test.completedvideo where userid = ?;`, m.UserId).Iter()
		iter2.Scan(&compltVid)

		flag := true
		for _, val := range compltVid {
			if val == m.VideoId {
				flag = false
				break
			}
		}

		if flag {
			err = session.Query(`update test.completedvideo set videoids = videoids + ? where userid=?;`, add, m.UserId).Exec()
			if err != nil {
				log.Println("err: ", err)
			}
		}

	} else {
		log.Println("something went wrong")
	}

}
