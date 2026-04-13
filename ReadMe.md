PA2 Map-Reduce I 

This program is an implementation of googles Map-Reduce
Takes in an input file and outputs key value pairs based on 
word frequency

file path =========

common /
  data.go

coordinator / 
  server.go
  driver.go

worker /
  client.go


How to run =========

  1.) Run the server (coordinator) by going into the coordinator path 
      and running go run .
  2.) Split Multiple terminals and run client (worker) by going into the worker path
      and running go run .


The coordinator makes M split files with split_M
The worker will be given an split_M and create key value pairs for word frequency

once all map splits have been successfully read, the coordinator will pass in an R task and an id
each R task will read from each column of mr-#-R  where R is reducer id

Finally all the reducers will combine the key value pairs and output to a output-R file


