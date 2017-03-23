# MongoDB-Tailer

A simple Go Lang tool to tail the actions that occur on the Mongo DB server by monitoring the op log of the DB.

For the oplog to be made available. The server needs to be started as a replcation set using the '--replSet' option

TODO:
	need to carry out testing on last commit, add logger tweaks, and fully flesh out the tailers system/setting vs single tailer
	need performance testing and performance tweaks.
	Elaborating functionality.
