package main

import (
	"fmt"
	"strconv"
	"encoding/json"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/mgo.v2/bson"
	"custom/mongo.tailer"
)

type Indexer struct {
	IndexName		string
	DocType 		string	  
}


//custome struct adjust it to the type of the documents that you are trying to index
type Article struct {
	Article_id 			int 			`bson:"article_id"`
	Article_title 		string 			`bson:"article_title"`
	Article_content 	string 			`bson:"article_content"`
	Creation_date 		string 			`bson:"creation_date"`
	Last_update 		string 			`bson:"last_update"`
	Author_id 			int 			`bson:"author_id"`
	Last_update_id 		int 			`bson:"last_update_id"`
	Author_name 		string 			`bson:"author_name"`
	Last_update_name 	string 			`bson:"last_update_name"`
}

func (w *Indexer) Index(updates chan tailer.Oplog){
	client, err := elastic.NewClient()
	if err != nil {
	    panic(err)
	}

	for document:=range updates {
		article := Article{}
		g, _ := json.Marshal(document.Document)
		json.Unmarshal(g, &article)
	
		_, err = client.Index().
		    Index(w.IndexName).
		    Type(w.DocType).
		    Id(strconv.Itoa(article.Article_id)).
		    BodyJson(article).
		    Do(context.Background())

		if err != nil {
		    panic(err)
		}

		fmt.Println("Indexed:",article)
	}
	
}

func main(){
	updates := make(chan tailer.Oplog, 10)
	r := &tailer.Reader{Url:"localhost:27017", Look_for:bson.M{"op": "i", "ns":"test.articles"}}
	w := &Indexer{IndexName:"tdid", DocType:"article"}
	
	go r.Monitor(updates)
	w.Index(updates)
}
