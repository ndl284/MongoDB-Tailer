package tailer

import (
	"errors"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "os"
    "time"
)

const OP_LOG = "oplog.rs"
const OP_LOG_COLLECTION = "local"

const READER_DEBUG_LOG = "debug"
const READER_PROD_LOG = "production"

const LOG_ERROR_TYPE = "[error]"
const LOG_DEBUG_TYPE = "[logger]"

const DEFAULT_LOG_FILE = "tailer.log"
const DEFAULT_LOG_FILE_PATH = ""

const TAILER_STOPPED_ERROR = "Tailer is stopped"

var Tailers map[int]*Reader = make(map[int]*Reader)
var maxId = 0

//struct to hold configuration details regarding the server to be tailed. If db has a username and password
// it can be added to this struct
type Reader struct {
	id int
	url string						//the database url
	look_for bson.M					//filter which actions should be retrived. E.g. insert/update/delete
	mode string
	stop bool
	err error
	logfile string
	logfilepath string
}

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

//The function to configure the url of the MongoDb instance. Stops the Monitor method if it is currenly running.
// INPUT
//		** url -  the url of the MongoDb instance
// RETURNS
//		** NONE
//
func (r *Reader) Url(url string){
	r.Stop()
	r.url = url
}

//The function to assign ids to the go lang threads running and adds it to the map of existing threads against it's id.
//	needs to be explicitly where there is a requirement to keep track of all existing threads
//	TODO: implement setting to enforce afromentioned system if required. 5%
// INPUT
//		** NONE
// RETURNS
//		** id - the id of the current tailer
//
func (r *Reader) AssignId() int{
	if r.id == 0 {
		maxId+=1
		r.id = maxId
	}
	Tailers[r.id] = r
	return r.id
}

//The function to assign a query to find only particular actions on the mongodb instance. Stops Monitor method.
// INPUT
//		** query - bson.M object containing a mongoDb query following the structure of the oplog.rs collection in the 
//					local db
// RETURNS
//		** NONE
//
func (r *Reader) AssignQuery(query bson.M){
	r.Stop()
	r.look_for = query
}

//The function to change the stop option to false (after the tailer has been stopped) and remove any errors on it. 
// The Monitor method needs to be called explicitly after this to restart the process.
// INPUT
//		** NONE
// RETURNS
//		** NONE
//
func (r *Reader) MakeReady(){
	r.stop = false;
	r.err = nil
}

//The function to change the stop option to true. This in turn will trigger a condition in the Monitor method to 
//	stop the method.
// INPUT
//		** NONE
// RETURNS
//		** NONE
//
func (r *Reader) Stop(){
	r.stop = true;
}


//The function to remove an object from the map of existing tailers. To be used if keeping track of the tailers.
// TODO: need to refine the collective system as mentioned in [AssignId.TODO]
// INPUT
//		** NONE
// RETURNS
//		** NONE
// 
func (r *Reader) Remove(){
	r.Stop()
	delete(Tailers, r.id)
}

//The function returns a boolean value to specify whether there is an error on the current tailer.
// INPUT
//		**NONE
// RETURNS
//		** returns true if the tailer has encountered an error, else returns false.
//
func (r *Reader) HasError() bool {
	if r.err!=nil {
		return true
	}
	return false
}

//The function returns an error that is encountered by the tailer, if any.
// INPUT
//		**NONE
// RETURNS
//		** returns the error that the tailer might have encountered, else returns nil
func (r *Reader) Error() error {
	return r.err;
}

//The function starts the tailer, making it monitor the mongoDB instance that has been configured.
// INPUT
//		** updates - Oplog channel that is used to pass the retreived data to functionality that consumes
//						this feature.
// RETURNS
//		** NONE
//
func (r *Reader) Monitor( updates chan Oplog){
	session, err := mgo.Dial(r.url)
	defer close(updates)

	if err != nil {
		r.err = err
		r.Logger(LOG_ERROR_TYPE, r.err.Error()) 
		panic(err) 
	}
	defer session.Close()

	obj := Oplog{}
	r.stop = false;
	session.SetMode(mgo.Monotonic, true)
	collection := session.DB(OP_LOG_COLLECTION).C(OP_LOG)
	tailable_cursor := collection.Find(r.look_for).Tail(-1)

	for {
		success:=tailable_cursor.Next(&obj)
		if success && !r.stop {
			r.Logger(LOG_DEBUG_TYPE, obj.Document.(string))
			updates <-obj
		} else {
			if(r.stop){
				r.err = errors.New(TAILER_STOPPED_ERROR)
			} else {
				r.err= tailable_cursor.Err()	
			}

			r.Logger(LOG_ERROR_TYPE, r.err.Error())
			panic(r.err)
			break
		}
	}

}

//The function to write to the log file while tailing mongo Db. The mode needs to be set while
//	initializing the object either READER_DEBUG_LOG | READER_PROD_LOG. In READER_PROD_LOG
//	only log messages of type LOG_ERR0R_TYPE will be logged.
//INPUT
//		** logtype - type of the log message, information or error (LOG_DEBUG_TYPE|LOG_ERROR_TYPE)
//		** content - the message that is to be logged
func (r* Reader) Logger(logtype string, content string){
	if logtype == LOG_DEBUG_TYPE && r.mode == READER_PROD_LOG {
		return
	}
    
    if r.logfile == "" {
    	r.logfile = DEFAULT_LOG_FILE
    }

	var file *os.File;
	var err error;

	if file, err = os.Open(r.logfile); err != nil {
		file, err = os.Create(r.logfile)
		if err != nil {
			panic(err)
		}
	}
	
	log := []byte("["+time.Now().String()+"]"+logtype+content+"\n")
	file.Write(log)
	file.Close()
}
