package main

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func mainCon(UserID gocql.UUID) [10]string {

	// connect to the cluster
	cluster := gocql.NewCluster("localhost:9042") //replace PublicIP with the IP addresses used by your cluster.
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()

	var topten [10]string

	if err != nil {
		log.Println(err)

	}
	defer session.Close()

	// create keyspaces
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)

	}

	// create table
	err = session.Query("CREATE TABLE IF NOT EXISTS test.userWatchedTags (userid uuid, tags map<text,int>, PRIMARY KEY (userid));").Exec()
	if err != nil {
		log.Println(err)

	}

	var userid gocql.UUID
	var tags map[string]int
	result := session.Query("select userid from test.userwatchedTags where userid=?", UserID).Iter()
	result.Scan(&userid)

	// validating the user
	if (userid != gocql.UUID{00000000 - 0000 - 0000 - 0000 - 000000000000}) {
		iter := session.Query("select tags from test.userwatchedTags where userid=?", UserID).Iter()
		iter.Scan(&tags)

		if iter == nil {
			log.Println("Something went wrong, Show most viewed videos for the user")
		} else {
			//sorting the tags by value
			type kv struct {
				Key   string
				Value int
			}

			var ss []kv
			for k, v := range tags {
				ss = append(ss, kv{k, v})
			}

			sort.Slice(ss, func(i, j int) bool {
				return ss[i].Value > ss[j].Value
			})

			var i = 0
			for _, kv := range ss {

				if i < 10 {
					topten[i] = kv.Key
					i++
				}
			}

		}

	} else {
		log.Println("Show most viewed videos for Guest user")
	}

	return topten
}

func VideoGenerator(arr [10]string, userid gocql.UUID) []gocql.UUID {
	// connect to the cluster
	cluster := gocql.NewCluster("localhost:9042") //replace PublicIP with the IP addresses used by your cluster.
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "cassandra", Password: "cassandra"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)

	}
	defer session.Close()

	// create keyspaces if not exit
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)

	}

	//seperating geners and tags
	var geners []string
	var tags []string
	for _, str := range arr {
		if strings.HasPrefix(str, "#") {
			tags = append(tags, str)
		} else if str == "" {
			continue
		} else {
			geners = append(geners, str)
		}
	}

	fmt.Println("geners", geners, "tags", tags)

	var videoIdList []gocql.UUID
	var randVideoIdList []gocql.UUID
	var generVideoIdList []gocql.UUID
	var tagVideoIdList []gocql.UUID
	var videoid gocql.UUID

	var lang []string
	langiter := session.Query(`select lang from test.user_table where userid = ?;`, userid).Iter()
	langiter.Scan(&lang)

	if len(lang) > 0 {
		for _, lng := range lang {
			//seraching geners and save the matched videoid's in generVideoIdList
			for _, a := range geners {
				iter := session.Query(`select videoid from test.video_table where lang=? and geners contains ? allow filtering;`, lng, a).Iter().Scanner()
				for iter.Next() {
					iter.Scan(&videoid)
					generVideoIdList = append(generVideoIdList, videoid)
				}

			}

			//seraching tags and save the matched videoid's in tagVideoIdList
			for _, a := range tags {
				iter := session.Query(`select videoid from test.video_table where lang=? and tags contains ? allow filtering;`, lng, a).Iter().Scanner()
				for iter.Next() {
					iter.Scan(&videoid)
					tagVideoIdList = append(tagVideoIdList, videoid)

				}

			}
		}

		//combining both gener and video id list's and removing duplicates from it
		check := make(map[gocql.UUID]int)
		d := append(generVideoIdList, tagVideoIdList...)

		for _, val := range d {
			check[val] = 1
		}

		for letter := range check {
			videoIdList = append(videoIdList, letter)
		}

		//removing completed videos from the suggestion list
		var compltVid []gocql.UUID
		iter2 := session.Query(`select videoids from test.completedvideo where userid = ?;`, userid).Iter()
		iter2.Scan(&compltVid)
		newlist := Difference(videoIdList, compltVid)

		//randomly picking non repeated suggestion list
		s := rand.NewSource(time.Now().Unix())
		r := rand.New(s)
		var chk []int
		for i := 0; i < 200; i++ {
			var randnum = r.Intn(len(newlist))
			if !contains(chk, randnum) {
				randVideoIdList = append(randVideoIdList, newlist[randnum])
				chk = append(chk, randnum)
			}
		}
	}

	return randVideoIdList
}

// function to check index is already in the list or not
func contains(s []int, st int) bool {
	for _, v := range s {
		if v == st {
			return true
		}
	}
	return false
}

//function to find the difference btw two slices
func Difference(a, b []gocql.UUID) (diff []gocql.UUID) {
	m := make(map[gocql.UUID]bool)

	for _, item := range b {
		m[item] = true
	}

	for _, item := range a {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return
}
