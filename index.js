var _expressPackage = require("express");
var _bodyParserPackage = require("body-parser");
var _sqlPackage = require("mssql");
var https = require("https");

var app = _expressPackage();

app.use(_bodyParserPackage.json());

app.use(function (req, res, next) {
   res.header("Access-Control-Allow-Origin", "*");
   res.header("Access-Control-Allow-Methods", "GET,POST");
   res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, contentType, Content-Type, Accept, Authorization");
   next();
});

var server = app.listen(process.env.PORT || 4000, function(){
   var port = server.address().port;
   console.log("App now runing on port", port);
});

// var dbConfig = {
//     user: "Admin",
//     password: "Admin123",
//     server: "DESKTOP-L115F7C",
//     port: 49175,
//     database: "testsql2"
//
// };

var dbConfig = {
    user: "AvihuPinko",
    password: "@avi1990pin",
    server: "avihupinko.database.windows.net",
    port: 1433,
    database: "avihupinko",
    options: {
        encrypt: true
    }
};

var QueryToExectueInDatabase = function (response , strQuery) {
    _sqlPackage.close();
    _sqlPackage.connect(dbConfig, function (error){
        if (error){
            console.log("Error While connecting to database :- " + error);
            response.send(error);
        } else {
            var request = new _sqlPackage.Request();

            request.query( strQuery , function (error, responseResult) {
               if (error){
                   console.log("Error While connecting to database :- " + error);
                   response.send(error);
               } else {
                   response.send(responseResult.recordset)
               }
            });
        }
    });
};

var ExectueQueryInDatabase = function (response , strQuery) {
    _sqlPackage.close();
    _sqlPackage.connect(dbConfig, function (error){
        if (error){
            console.log("Error While connecting to database :- " + error);
            response.send(error);
        } else {
            var request = new _sqlPackage.Request();

            request.query( strQuery , function (error, responseResult) {
                if (error){
                    console.log("Error While connecting to database :- " + error);
                    response.send(error);
                } else {
                    response.send("OK");
                }
            });
        }
    });
};

app.get("/test", function(_req, _res) {
    _res.send("TEST_OK");
});


app.get("/createDB", function(_req, _res){
    var sqlQuery = "CREATE TABLE campaigns" +
        "id [varchar](50) NOT NULL," +
        "platform [varchar](50) NULL,\n" +
        "country [varchar](50) NULL,\n" +
        "payout [float] NULL,\n" +
        "click_url [varchar](max) NULL,\n" +
        "currency [varchar](50) NULL,\n" +
        "Android_package_name [varchar](50) NULL,\n" +
        "appstore_url [varchar](max) NULL,\n" +
        "ios_bundle_id [varchar](50) NULL,\n" +
        "estimated_hops [float] NULL,\n" +
        "device_id_required [int] NULL,\n" +
        "pulled_date [datetime] NULL,\n" +
        "active [int] NULL)";
    QueryToExectueInDatabase(_res, sqlQuery);
});


app.get("/platform", function(_req, _res){
    var where = '';
    if (_req.query.os){
        where = ' AND platform = \'' + _req.query.os + '\'';
    }
   var sqlQuery = "select * from campaigns where active = 1 " + where;
   QueryToExectueInDatabase(_res, sqlQuery);
});

/* get all payout unless lt or gt are given */
app.get("/payout", function(_req, _res){
    var sqlQuery = "select * from campaigns where active = 1 ";
    var where = '';

    if (_req.query.lt){
        where += ' payout <= ' + _req.query.lt;
        if (_req.query.gt){
            where += ' AND payout >= ' + _req.query.gt;
        }
    }
    else if (_req.query.gt){
        where += ' payout >= ' + _req.query.gt;
    }
    if (where !== ''){
        sqlQuery += ' AND ' + where;
    } else {
        sqlQuery += ' order by payout'
    }
    QueryToExectueInDatabase(_res, sqlQuery);
});

app.get("/offers", function(_req, _res){
    var sqlQuery = "select * from campaigns where active = 1 ";
    var where = '';

    if (_req.query.lt){
        where += ' AND payout <= ' + _req.query.lt;
    }
    if (_req.query.gt){
        where += ' AND payout >= ' + _req.query.gt;
    }
    if (_req.query.os){
        where = ' AND platform = \'' + _req.query.os + '\'';
    }
    if (where !== ''){
        sqlQuery += where;
    } else {
        sqlQuery += ' order by payout'
    }
    QueryToExectueInDatabase(_res, sqlQuery);
});

app.get("/update", function(_req, _res){
    https.get("https://feed.appthis.com/v2?api_key=88a5613ffc85192b7ccca77ac0725719", (resp) => {
       let data = '';
       resp.on('data', (chunk) => {
          data += chunk;
       });

       resp.on('end', ()=>{
           var jsonData = JSON.parse(data);
           var result = jsonData.campaigns.filter( obj => obj.conversions.limit_daily > 99 && obj.metrics.stability > 14 && obj.metrics.estimated_hops < 5);

           updateDB(_res, result);
           // _res.send(result);
       });
    }).on("error", (err) => {
        console.log("Error: " + err.message);
    });
});

var updateDB = async function (_res , array) {
    var query = '';
    var id_arr = [];
    for (const obj of array ) {
         query += createQuery(obj);
         id_arr.push(rot5(obj.id));
    }

    // update all campaigns who are not in the data pulled to be not active
    query += 'update campaigns set active = 0 where id not in (' + id_arr.join(', ') + ')';
    // console.log('number of compaigns = ' + array.length);
    ExectueQueryInDatabase(_res, query);
};

var rot5 = function (id){
    var r5_id = 0;
    id = id.toString();
    for (var i=0; i< id.length; i++){
        r5_id = r5_id * 10 + ((Number(id[i]) + 5) % 10);
    }
    return r5_id;
}

var createQuery = function (obj) {
    var query = '';
    var click_url = "";
    var regex ;

    if ((obj.targeting.os.values[0]).toLowerCase() == 'ios'){
        console.log('ios');
        regex = /\&androidid={ANDROID_ID}/i;
        click_url = obj.click_url.toString().replace(regex, '');
        console.log(click_url);
    } else {
        console.log('andoroid');
        regex = /\&idfa={IDFA}/i;
        click_url = obj.click_url.toString().replace(regex, '');
        console.log(click_url);
    }
    var id = rot5(obj.id);
    query = 'IF EXISTS ( SELECT * FROM campaigns where id = ' + id + ') ' + "\n" +
                'UPDATE campaigns SET '+
                    'platform = \'' +obj.targeting.os.values + '\', country = \'' + obj.targeting.country.values + '\','+
                    'payout = '+ obj.payout.amount + ', click_url = \''+ click_url + '\' , currency = \'' + obj.payout.currency + '\',' +
                    ' Android_package_name = \'' + obj.creative.native.android_package_name + '\', appstore_url = \'' + obj.creative.native.appstore_url + '\','+
                    ' ios_bundle_id = ' + obj.creative.native.ios_bundle_id + ', estimated_hops = ' + obj.metrics.estimated_hops +
                    ', device_id_required = '+(obj.targeting.device_id_requierd ? 1 : 0 ) + ', pulled_date = GETDATE() ,active = 1 '+ "\n" +
                'WHERE id = \''+ id + "'\n" +
            'ELSE '+ "\n" +
                'INSERT INTO campaigns (id ,platform, country ,payout, click_url , currency , Android_package_name, appstore_url,' +
                ' ios_bundle_id , estimated_hops, device_id_required , pulled_date ,active) ' + "\n" +
                'VALUES (\'' + id + '\',\'' +obj.targeting.os.values + '\',\'' + obj.targeting.country.values + '\','+
                obj.payout.amount+', \''+click_url+'\', \''+ obj.payout.currency + '\', \'' + obj.creative.native.android_package_name  + '\',\''+
                obj.creative.native.appstore_url + '\',\'' + obj.creative.native.ios_bundle_id + '\',' + obj.metrics.estimated_hops + ',' +
                (obj.targeting.device_id_requierd ? 1 : 0 ) + ', GETDATE() , 1 )' + "\n";


    return query;
};