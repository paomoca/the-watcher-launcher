var express = require('express')
var MongoClient = require('mongodb').MongoClient
var moment = require('moment-timezone')

var launch_routine = require('./launch-routine.js')
var launcher = require('./launch.js')


var app = express()

// Connection URL
//var url = 'mongodb://172.16.73.145:27017/juan';
//myproject
//the-watcher
var url = 'mongodb://localhost:27017/the-watcher';
//var url = 'mongodb://juan:juanito@ds057176.mlab.com:57176/paomoca-tests'

// Use connect method to connect to the server
MongoClient.connect(url, function(err, database) {

  console.log("Connected successfully to server")
  db = database


  // var meses = []
  // var dias = []
  // var horas = []
  // var arr = [{ d: 4, h:6, value: 0.75 },{d: 9, h:7, value: 1.75 },{d: 13, h:8, value: 2.75 }]
  // arr.map(function(currentValue, index, arr){
  //   console.log(currentValue.d);
  //   console.log(currentValue.h);
  //   console.log(currentValue.value);
  //
  //
  //
  //   console.log('\n')
  // })

  launcher.run(db, function(msg){
    console.log(msg)
  })

});



app.listen(3005, function () {
  console.log('Example app listening on port 3000!')
})
