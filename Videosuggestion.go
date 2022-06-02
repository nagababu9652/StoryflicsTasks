package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/gocql/gocql"
)

type Videoschema struct {
	VideoId gocql.UUID `json:"VideoId"`
	UserId  gocql.UUID `json:"UserId"`
}

func Videosuggestion(jbyte []byte) []gocql.UUID {

	// connect to the cluster
	cluster := gocql.NewCluster("localhost:9042") //list of IP addresses used by your cluster.
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
	}
	defer session.Close()

	// create keyspaces if not available
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)
	}

	// unmarshal json and save in msg
	var msg = []byte(jbyte)
	m := Videoschema{}
	json.Unmarshal(msg, &m)

	// validate user and video uuid types
	userstr := gocql.UUID.String(m.UserId)
	var getuserid, err1 = gocql.ParseUUID(userstr)
	if err1 != nil {
		log.Println(err)
	}

	videoidstr := gocql.UUID.String(m.VideoId)
	var getvidoid, err2 = gocql.ParseUUID(videoidstr)
	if err2 != nil {
		log.Println(err)
	}

	var lang []string
	langiter := session.Query(`select lang from test.user_table where userid = ?;`, m.UserId).Iter()
	langiter.Scan(&lang)

	var geners []string
	var tags []string
	var videoIdList []gocql.UUID
	var randVideoIdList []gocql.UUID
	var generVideoIdList []gocql.UUID
	var tagVideoIdList []gocql.UUID
	var videoid gocql.UUID

	invalidUUID, _ := gocql.ParseUUID("00000000-0000-0000-0000-000000000000")
	if getuserid != invalidUUID && getvidoid != invalidUUID {
		iter := session.Query(`select geners,tags from test.video_table where videoid= ?;`, m.VideoId).Iter()
		iter.Scan(&geners, &tags)

		for _, lng := range lang {
			//seraching geners and save the matched videoid's in generVideoIdList
			for _, a := range geners {
				iter1 := session.Query(`select videoid from test.video_table where lang=? and geners contains ? allow filtering;`, lng, a).Iter().Scanner()
				for iter1.Next() {
					iter1.Scan(&videoid)
					generVideoIdList = append(generVideoIdList, videoid)
				}

			}

			for _, a := range tags {
				iter1 := session.Query(`select videoid from test.video_table where lang=? and tags contains ? allow filtering;`, lng, a).Iter().Scanner()
				for iter1.Next() {
					iter1.Scan(&videoid)
					tagVideoIdList = append(tagVideoIdList, videoid)
				}

			}

		}
		//combining both list's
		d := append(generVideoIdList, tagVideoIdList...)

		//removing duplicates from it
		check := make(map[gocql.UUID]int)
		for _, val := range d {
			check[val] = 1
		}

		for letter := range check {
			videoIdList = append(videoIdList, letter)
		}

		//removing completed videos from the suggestion list
		var compltVid []gocql.UUID
		iter2 := session.Query(`select videoids from test.completedvideo where userid = ?;`, m.UserId).Iter()
		iter2.Scan(&compltVid)
		newlist := Difference(videoIdList, compltVid)

		//randamize the results
		s := rand.NewSource(time.Now().Unix())
		r := rand.New(s)
		var chk []int
		for i := 0; i < 100; i++ {
			var randnum = r.Intn(len(newlist))
			if !contains(chk, randnum) {
				randVideoIdList = append(randVideoIdList, newlist[randnum])
				chk = append(chk, randnum)
			}
		}

	} else if getvidoid != invalidUUID {
		log.Println("something went wrong may be guest user")

		var lang string
		iter := session.Query(`select geners,tags, lang from test.video_table where videoid= ?;`, m.VideoId).Iter()
		iter.Scan(&geners, &tags, &lang)

		for _, a := range geners {
			iter1 := session.Query(`select videoid from test.video_table where lang=? and geners contains ? allow filtering;`, lang, a).Iter().Scanner()
			for iter1.Next() {
				iter1.Scan(&videoid)
				generVideoIdList = append(generVideoIdList, videoid)
			}

		}

		for _, a := range tags {
			iter1 := session.Query(`select videoid from test.video_table where lang=? and tags contains ? allow filtering;`, lang, a).Iter().Scanner()
			for iter1.Next() {
				iter1.Scan(&videoid)
				tagVideoIdList = append(tagVideoIdList, videoid)
			}

		}

		//combining both list's
		d := append(generVideoIdList, tagVideoIdList...)

		//removing duplicates from it
		check := make(map[gocql.UUID]int)
		for _, val := range d {
			check[val] = 1
		}

		for letter := range check {
			videoIdList = append(videoIdList, letter)
		}

		//randamize the results
		s := rand.NewSource(time.Now().Unix())
		r := rand.New(s)
		var chk []int
		for i := 0; i < 100; i++ {
			var randnum = r.Intn(len(videoIdList))
			if !contains(chk, randnum) {
				randVideoIdList = append(randVideoIdList, videoIdList[randnum])
				chk = append(chk, randnum)
			}
		}

	} else {
		log.Println("Something went wrong")
	}

	return randVideoIdList
}
