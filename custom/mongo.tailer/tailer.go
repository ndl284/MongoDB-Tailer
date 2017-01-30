package tailer

import (
    "time"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
)

//struct to unmarshall the data coming from the oplog table in mongodb
type Oplog struct {
	Timestamp 		bson.MongoTimestamp `bson:"ts"`			// timestamp of when the action occurred
	limitin 		int					`bson:"t"`			//
	HistoryID		int64 				`bson:"h"`	
	MongoVersion	int 				`bson:"v"`			//MongoDB version
	Operation 		string 				`bson:"op"`			//The operation that is performed.
	Namespace 		string 				`bson:"ns"`			//Namespace, the table being targeted. format dbname.tablename
	Document 		interface{}	   		`bson:"o"`			//The payload of the action.
}

//struct to hold configuration details regarding the server to be tailed. If db has a username and password
// it can be added to this struct
type Reader struct {
	Url string							//the database url
	Look_for bson.M 					//filter which actions should be retrived. E.g. insert/update/delete
}

//The function tails the oplog and retrieves the actions that occur on the db.
// Input
//		** updates - a channel of type Oplog that is used to pass the retrieved documents.
func (r *Reader) Monitor( updates chan Oplog){
	session, err := mgo.Dial(r.Url)
	defer close(updates)

	if err != nil { 
		panic(err) 
	}
	defer session.Close()
	obj := Oplog{}

	session.SetMode(mgo.Monotonic, true)
	collection := session.DB("local").C("oplog.rs")
	tailable_cursor := collection.Find(r.Look_for).Tail(-1)

	for {
		success:=tailable_cursor.Next(&obj)
		if success {
			updates <-obj
		} else {
			if tailable_cursor.Err() != nil {
				panic(tailable_cursor.Err())
			}
			time.Sleep(10000 * time.Millisecond)
		}
	}

}
