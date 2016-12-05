var ObjectId = require('mongodb').ObjectID
var statistics = require('./calculate-statistics-launcher.js')
var moment = require('moment-timezone')

/*Inicia en launch(), por cada variable en la db realiza las siguientes acciones:

1. Loop desde el anio inicial (2016) hasta el anio actual
2. Para cada anio que existe genera las estadisticas del anio (1 valor por mes) y hace un loop de sus meses
3. Para cada mes que exista genera las estadisticas del mes (1 valor por dia) y hace un loop de sus dias
4. Para cada dia que exista genera las estadisticas del dia (1 valor por hora)

*/

//1 Enero 2016 00:00
//var minUTCTimestamp = 1451606400000
//var minUTC = new Date(minUTCTimestamp)
var minObj = { year : 2016, month : 0, day : 1, hour: 0 };


var run = function(db, callback){

  var minYear = 2016
  var year = new Date().getUTCFullYear()

  var now = new Date().getTime()

  // db.collection('cron-logs').findOne(function(err, doc){
  //   if(!err && doc){
  //     minUTCTimestamp = doc.lastCronTimestamp
  //     console.log(minUTCTimestamp)
  //   }
  // })


  db.collection('variables').find({}, {_id:1, timezone:1}).forEach(function(doc){

    var id = ObjectId(doc._id).toString()
    var variableDataCollection = db.collection(id)

    if(doc.timezone){
      var timezone = doc.timezone
    } else {
      var timezone = "America/Mexico_City"
    }

    var minLocal = moment.tz(minObj, timezone)
    var monthUTC = minLocal.clone().utc()
    var dayUTC = minLocal.clone().utc()

    do {

      nextMonthUTC = monthUTC.clone().add(1, 'month')
      runMonth(db, id, monthUTC, nextMonthUTC)
      monthUTC = nextMonthUTC

    } while(monthUTC < now)

  })

  callback('Launched all statistic calculations')

}

var runMonth = function(db, id, monthUTC, nextMonthUTC){

  statistics.monthStatistics(db, id, monthUTC.valueOf(), nextMonthUTC.valueOf(), function(dataMonth){

    console.log('came back MONTH '+dataMonth);
    if(dataMonth){

      console.log(id+': Running Days ')

      dayUTC = monthUTC.clone()

      do {

        nextDayUTC = dayUTC.clone().add(1, 'day')
        runDay(db, id, dayUTC, nextDayUTC)
        dayUTC = nextDayUTC

      } while(dayUTC < nextMonthUTC)

    }

  })

}

var runDay = function(db, id, dayUTC, nextDayUTC){

  statistics.dayStatistics(db, id, dayUTC.valueOf(), nextDayUTC.valueOf(), function(dataDay){

    console.log('came back DAY '+dataDay);

    if(dataDay){

      console.log(id+': Running Hours ')

      hourUTC = dayUTC.clone()

      do {

        nextHourUTC = hourUTC.clone().add(1, 'hour')
        runHour(db, id, hourUTC, nextHourUTC)
        hourUTC = nextHourUTC

      } while(hourUTC < nextDayUTC)

    }

  })
}

var runHour = function(db, id, hourUTC, nextHourUTC){

  statistics.hourStatistics(db, id, hourUTC.valueOf(), nextHourUTC.valueOf(), function(dataHour){
    //console.log('came back HOUR '+dataHour);
  })

}

var months = function(db, id, y, m, timezone){

  var obj = { year : y, month : m, day : 1, hour: 0 };

  var minLocal = moment.tz(obj, timezone)
  var maxLocal = minLocal.clone().add(1, 'month')

  var min = minLocal.clone().utc().valueOf()
  var max = maxLocal.clone().utc().valueOf()

  console.log(id+'------TRYING'+new Date(minLocal))

  var data = statistics.monthStatistics(db, id, min, max)

  if(data != 0){

    var daysInMonth = minLocal.daysInMonth()
    for(var d = 1; d <= daysInMonth; d++){

      days(db, id, y, m , d, timezone)

    }
  }

}

var days = function(db, id, y, m, d, timezone){

  var obj = { year : y, month : m, day : d, hour: 0 };

  var minLocal = moment.tz(obj, timezone)
  var maxLocal = minLocal.clone().add(1, 'day')

  var min = minLocal.clone().utc().valueOf()
  var max = maxLocal.clone().utc().valueOf()

  var data = statistics.dayStatistics(db, id, min, max)

  if(data != 0){
    for(var h = 0; h <= 23; h++){
      hours(db, id, y, m , d, h, timezone)
    }
  }

}

var hours = function(db, id, y, m, d, h, timezone){

  var obj = { year : y, month : m, day : d, hour: h };

  var minLocal = moment.tz(obj, timezone)
  var maxLocal = minLocal.clone().add(1, 'hour')

  var min = minLocal.clone().utc().valueOf()
  var max = maxLocal.clone().utc().valueOf()

  var data = statistics.hourStatistics(db, id, min, max)

}

exports.run = run
