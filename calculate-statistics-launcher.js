var ObjectId = require('mongodb').ObjectID
var statistics = require('simple-statistics')
var moment = require('moment-timezone')

var project = {
  $project:
  {
    value : 1,
    date : 1,
    _id : 0,
  }
}

var projectValue = {
  $project:
  {
    value : 1,
    _id : 0,
  }

}

var sort = {
  $sort : {
    value : 1
  }
}

var hourStatistics = function(db, id, minTimestamp, maxTimestamp, callback){

  var timestamp = new Date(minTimestamp)

  var match = {
    $match : {  $and: [
      { date: { $gte: new Date(minTimestamp) } },
      { date: { $lt: new Date(maxTimestamp) } }
    ] }
  }

  var collection = db.collection(id)

  var cursor = collection.aggregate([project,match, projectValue, sort]).toArray(function(err, docs) {

    if(!err && docs.length != 0){

      var mappedArray = docs.map(function (item) { return item.value; });
      generateStatisticsDocument(mappedArray, timestamp, function(results){

        db.collection(id+'-hours').update({timestamp: timestamp}, results, { upsert: true }, function(error,docs){
          if(error){
            console.log(id+': HOURS '+timestamp+' ERROR')
          } else {
            console.log(id+': HOURS '+timestamp)
          }
          callback(1)
        })

      })



    } else {
    //  console.log(id+': HOURS '+timestamp+' ERROR');
      callback(0)
    }

  });

}

var dayStatistics = function(db, id, minTimestamp, maxTimestamp, callback){

  var timestamp = new Date(minTimestamp)
  console.log(id+': Started DAY '+timestamp+' - '+new Date(maxTimestamp))

  var match = {
    $match : {  $and: [
      { date: { $gte: new Date(minTimestamp) } },
      { date: { $lt: new Date(maxTimestamp) } }
    ] }
  }

  var collection = db.collection(id)

  var cursor = collection.aggregate([project, match, projectValue, sort]).toArray(function(err, docs) {

    if(!err && docs.length != 0){

      var mappedArray = docs.map(function (item) { return item.value; })

      generateStatisticsDocument(mappedArray, timestamp, function(results){
        db.collection(id+'-day').update({timestamp: timestamp}, results, { upsert: true }, function(error,docs){
          if(error){
            console.log(id+': DAY '+timestamp+' ERROR')
          } else {
            console.log(id+': DAY '+timestamp)
          }
          callback(1)
        })
      })

    } else {
      console.log(id+': DAY '+timestamp+' ERROR');
      callback(0)
    }

  });


}

var monthStatistics = function(db, id, minTimestamp, maxTimestamp, callback){

  var timestamp = new Date(minTimestamp)

  console.log(id+': Started MONTH '+timestamp+' - '+new Date(maxTimestamp))

  var match = {
    $match : {  $and: [
      { date: { $gte: new Date(minTimestamp) } },
      { date: { $lt: new Date(maxTimestamp) } }
    ] }
  }

  var collection = db.collection(id)

  var cursor = collection.aggregate([project,match, projectValue, sort]).toArray(function(err, docs) {

    if(!err && docs.length != 0){

      var mappedArray = docs.map(function (item) { return item.value; });
      generateStatisticsDocument(mappedArray, timestamp, function(results){
        db.collection(id+'-month').update({timestamp: timestamp}, results, { upsert: true }, function(error,docs){
          if(error){
            console.log(id+': MONTH '+timestamp+' ERROR')
          } else {
            console.log(id+': MONTH '+timestamp)
          }
          callback(1)
        })
      })

    } else {
      console.log(id+': MONTH '+timestamp+' ERROR');
      callback(0)
    }


  });


}

var generateStatisticsDocument = function(array, timestamp, callback){

  var results = {}
  results.timestamp = timestamp
  results.mean = statistics.mean(array)
  results.mode = statistics.modeSorted(array)
  results.median = statistics.medianSorted(array)
  results.arrayCount = array.length
  results.createdAt = new Date()

   callback(results)

}

exports.monthStatistics = monthStatistics
exports.dayStatistics = dayStatistics
exports.hourStatistics = hourStatistics
