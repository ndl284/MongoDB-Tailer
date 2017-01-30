# MongoDB-Tailer

A simple Go Lang tool to tail the actions that occur on the Mongo DB server by monitoring the op log of the DB.

For the oplog to be made available. The server needs to be started as a replcation set using the '--replSet' option

Implimentation is still rudimentary. Logging needs to be implemented. Along with notifications incase of failure.
