# Clowder activity-monitor
Service for monitoring Clowder's event sink and uploading datapoints to a database.

Currently just pushes the events into a database. Example usage:

`docker build -f Dockerfile -t maxzilla2/activity-monitor .`

`docker run -e RABBITMQ_URI="amqp://max:test@host.docker.internal:5672/%2f" -e MONGODB_URI="mongodb://host.docker.internal:27017" maxzilla2/activity-monitor`

The Mongo database (`eventsink` by default) will look like this (I obviously changed ids):

> db.events.find()
{ "_id" : ObjectId("123"), "resource_name" : "RMQ Test", "resource_id" : "123", "author_id" : "123", "viewer_name" : "Max B mburnet2@illinois.edu", "type" : "dataset", "routing_key" : "event.sink", "author_name" : "Max B mburnet2@illinois.edu", "viewer_id" : "123" }

