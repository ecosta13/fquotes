/**
 * Created by lawrenceackner on 5/10/18.
 */

var request = require('request');
var mysql = require('mysql');
var moment = require('moment');
var tickCnt = 0;
var tickSize = 60;
let currentDate = null;
let currentPath;
let prefix = "Option";
let testProg = false;
let dbInfo = {
    host: "localhost",
    user: "root",
    password: "ccccccc",
    database: "Vproduction"

}
let dbInfoHive = {

    host: "mysql.hive31.net",
    user: "user",
    password: "passwd",
    database: "db"
}

let token = 'token';
let syms = [{
    s: 'rut',
    oh: 6,
    om: 30,
    ch: 13,
    cm: 18
}, {
    s: 'spx',
    oh: 6,
    om: 30,
    ch: 13,
    cm: 18
}
];
function getTableName(elem) {
    let weekly = "";
    let symbol = elem.symbol.split('_')[0].toLowerCase();
    let exp = elem.symbol.split('_')[1].substring(0, 6);
    let yr = exp.substring(4);
    exp = yr + exp.substring(0, 4);
    if (symbol.length == 4) {
        weekly = "W";
        symbol = symbol.substr(0, symbol.length - 1)

    }
    let name = prefix + "_" + symbol + "_" + weekly + exp;
    return name;

}
function createTable(table) {
    let string = "CREATE TABLE IF NOT EXISTS " + table + " (";
    string = string + " Date date DEFAULT NULL,";
    string = string + " Time time DEFAULT NULL,";
    string = string + " Type tinytext,";
    string = string + " StrikePrice decimal(8,2) DEFAULT NULL,"
    string = string + " Symbol tinytext,";
    string = string + " Mark decimal(8,2) DEFAULT NULL,";
    string = string + " Last decimal(8,2) DEFAULT NULL,";
    string = string + " Bid decimal(8,2) DEFAULT NULL,";
    string = string + " Ask decimal(8,2) DEFAULT NULL,";
    string = string + " Delta decimal(8,2) DEFAULT NULL,";
    string = string + " IV decimal(8,2) DEFAULT NULL,";
    string = string + " OpenInterest int(11) DEFAULT NULL,";
    string = string + " Volume bigint(20) DEFAULT NULL,";
    string = string + " tickCnt int(11) DEFAULT NULL,";
    string = string + " tickSize int(11) DEFAULT NULL,";
    string = string + " blockID int(11) DEFAULT NULL,";
    string = string + " SessionTS int(11) DEFAULT NULL,";
    string = string + " atmd decimal(8,2) DEFAULT NULL,";
    string = string + " Underlying tinytext,";
    string = string + " quoteSource varchar(8) DEFAULT NULL,";
    string = string + " id int(11) DEFAULT NULL";
    string = string + ")";

    return new Promise((resolve, reject) => {

        con.query(string, "", (err, rows) => {
            if (err)
                return reject(err);
            resolve();
        });
    });


}
function twoDigits(d) {
    if (0 <= d && d < 10) return "0" + d.toString();
    if (-10 < d && d < 0) return "-0" + (-1 * d).toString();
    return d.toString();
}

Date.prototype.toMysqlFormat = function () {
    return this.getUTCFullYear() + "-" + twoDigits(1 + this.getUTCMonth()) + "-" + twoDigits(this.getUTCDate()) + " " + twoDigits(this.getHours()) + ":" + twoDigits(this.getUTCMinutes()) + ":" + twoDigits(this.getUTCSeconds());
};
function getHeaderString() {

    var hdr = "Date,Time,Type,StrikePrice,Symbol,Mark,Last,Bid,Ask,Delta,IV,OpenInterest,Volume," +
        "tickCnt,tickSize,blockID,sessionTS,atmd,Underlying,quoteSource,id\n"
    return hdr;


}
function getRowString(elem, underlyingPrice,symbol, symraw, dt, tm) {
    let typ2;
    var MyDate = new Date();

    let typ = elem.putCall == "CALL" ? 'C' : 'P';

    let atmd = (  underlyingPrice  - elem.strikePrice).toFixed(2);

    let sym = symraw ;
    var row =
        dt + "," +
        tm + "," +
        typ + "," +
        elem.strikePrice + "," +
        sym + "," +
        elem.mark + "," +
        elem.last + "," +
        elem.bid + "," +
        elem.ask.toFixed(2) + "," +
        elem.delta.toFixed(2) + "," +
        elem.volatility.toFixed(2) + "," +
        elem.openInterest + "," +
        elem.totalVolume + "," +
        tickCnt + "," +
        tickSize + "," +
        "0,0," +
        atmd + "," + symbol.toUpperCase() + ",ATRD,0\n"
    return row;


}

let active = false;
function connectToDB() {
    var con = mysql.createConnection(dbInfoHive);
    active = true;
    con.connect();
    return con;

}
function extractData(expMap, table_names, underlyingPrice, sym, map, tablesToCreate, dt, tm) {
    for (let item in expMap) {
        let strikes = expMap[item];
        for (let idx in strikes) {
            let elems = strikes[idx];
            for (let i in elems)
                addData(elems[i], table_names, underlyingPrice, sym, map, tablesToCreate, dt, tm);
        }
    }
}
function getInsertString(underlyingPrice, symraw, dt, tm) {
    var MyDate = new Date();


    let sym = "'" + symraw + "'";
    var sql = "INSERT INTO " + prefix + "_Underlying ( Date,Time, Symbol, Price, blockID,quoteSource,id) " +
        " VALUES (" +
        "'" + dt + "'," +
        "'" + tm + "'," +
        sym + "," +
        underlyingPrice + "," +
        "0,'ATRD',0" +
        ")";
    return sql;


}
function insertIntoTable(underlyingPrice, sym, dt, tm) {

    var sql = getInsertString(underlyingPrice, sym, dt, tm);
    return new Promise((resolve, reject) => {
        con.query(sql, "", (err, rows) => {
            if (err)
                return reject(err);
            resolve();
        });
    });

}
function getFromAmeritrade(table_names, sym, sc, map, tablesToCreate, dt, tm) {
    request('https://smartdocs.tdameritrade.com/smartdocs/v1/sendrequest?targeturl=https%3A%2F%2Fapi.tdameritrade.com%2Fv1%2Fmarketdata%2Fchains%3Fapikey%3DFOLARTS%2540AMER.OAUTHAP%26symbol%3D' + sym + '%26strikeCount%3D' + sc + '&_='+token , function (error, response, body) {
        if (!error && response.statusCode == 200) {

            let res = null;
            try {
                res = JSON.parse(body);
            }
            catch (err) {
                console.log("ERROR ERROR " + err)
                return;
            }

            res = decodeURIComponent(res.responseContent);
            res = JSON.parse(res);
            if(res.underlyingPrice <1) {
                console.log("WARNING price is low " + res.underlyingPrice);
                return;
            }
            insertIntoTable(res.underlyingPrice.toFixed(2), res.symbol.toUpperCase(), dt, tm).then(function () {

                extractData(res.callExpDateMap, table_names, res.underlyingPrice, sym, map, tablesToCreate, dt, tm);
                extractData(res.putExpDateMap, table_names, res.underlyingPrice, sym, map, tablesToCreate, dt, tm);

                writeTempFiles(map, sym,res.underlyingPrice,dt, tm);
                bulkLoad(map, tablesToCreate, sym, tm)
            }).catch(function (err) {
                console.log("ERROR ERROR insert underlying table " + err);
                extractData(res.callExpDateMap, table_names, res.underlyingPrice, sym, map, tablesToCreate, dt, tm);
                extractData(res.putExpDateMap, table_names, res.underlyingPrice, sym, map, tablesToCreate, dt, tm);

                writeTempFiles(map, sym,res.underlyingPrice,dt, tm);
                active = false;
                if (con)
                    con.end();
                con = null;
                return;
            });

        } else {
            console.log('OOOOOOOOO');
        }
    });
}
function getAllTables2(con) {
    return new Promise((resolve, reject) => {
        let sql = "SELECT * FROM information_schema.tables WHERE table_schema = '" + dbInfoHive.database + "' and table_name LIKE '" + prefix + "%' ";

        con.query(sql, "", (err, rows) => {
            if (err)
                return reject(err);
            resolve(rows);
        });
    });
}


function addData(elem, table_names, underlyingPrice, sym, map, tablesToCreate, dt, tm) {

    let tbl = getTableName(elem);

    if (!table_names.includes(tbl)) {
        table_names.push(tbl);
        tablesToCreate.push(tbl);
    }
    if (map[tbl] == undefined) {
        map[tbl] = [];
        tbl_cnt = tbl_cnt + 1;
    }
    map[tbl].push(getRowString(elem, underlyingPrice,sym, tbl.split('_')[2], dt, tm));


}
function writeTempFiles(map, sym,underlyingPrice,dt, tm) {
    let fs = require('fs');
    let dir = __dirname + "/";
    let fn = currentPath + "/" + sym + "/underlying.csv";
    let logStream = fs.createWriteStream(fn, {'flags': 'a'})
    logStream.end(dt + ","+ tm + ","+ sym.toUpperCase()+ ","+ underlyingPrice.toFixed(2)+ ",0,ATRD,0\n");

    for (tbl in map) {
        fn = currentPath + "/" + sym + "/" + tbl + "_" + tm.replace(/\:/g, '-') + ".csv";
        logStream = fs.createWriteStream(fn, {'flags': 'w'});

        //logStream.write(getHeaderString());
        for (idx in map[tbl]) {
            if (idx == map[tbl].length - 1)
                logStream.end(map[tbl][idx]);
            else
                logStream.write(map[tbl][idx]);
        }

    }

}
let cnt = 0;
function bulkLoad(map, tablesToCreate, sym, tm) {
    let dir = __dirname + "/";
    for (tbl in map) {
        //let fn = dir + tbl + ".csv";
        let fn = currentPath + "/" + sym + "/" + tbl + "_" + tm.replace(/\:/g, '-') + ".csv";
        let str2 = "LOAD DATA LOCAL INFILE '" + fn + "' INTO TABLE " + tbl +
            " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES " +
            " (Date,Time,Type,StrikePrice,Symbol," +
            "Mark,Last,Bid,Ask,Delta,IV,OpenInterest,Volume," +
            "tickCnt,tickSize,blockID,sessionTS,atmd,Underlying,quoteSource,id)";
        let str = "LOAD DATA LOCAL INFILE '" + fn + "' INTO TABLE " + tbl + " (Date,Time,Type,StrikePrice,Symbol," +
            "Mark,Last,Bid,Ask,Delta,IV,OpenInterest,Volume," +
            "tickCnt,tickSize,blockID,sessionTS,atmd,Underlying,quoteSource,id)"
        if (tablesToCreate.includes(tbl)) {
            createTable(tbl).then(function () {


                con.query(str2,
                    [],
                    function (err) {
                        if (err == null)
                        {
                            console.log("1 table loaded" + tbl_cnt);
                            tbl_cnt = tbl_cnt - 1;
                            if (tbl_cnt == 0) {
                                console.log("end time =  " + moment().format('HH:mm:ss'));
                            }
                        } else {
                            console.log("ERROR ERROR bulk load " + err);
                            active = false;
                            if (con)
                                con.end();
                            con = null;
                        }
                    });
            });

        } else {
            con.query(str2,
                [],
                function (err) {
                    //console.log("1 table loaded "+cnt);
                    if (err == null) {
                        tbl_cnt = tbl_cnt - 1;
                        // console.log("1 table loaded "+ tbl_cnt);
                        if (tbl_cnt == 0) {
                            console.log("end time =  " + moment().format('HH:mm:ss'));
                        }
                    } else {
                        console.log("ERROR ERROR bulkload " + err)
                    }

                });
        }
    }


}


let table_names = [];
let_tbl_cnt = 0;
;
function removeCSVfiles() {
    var fs = require('fs');

    fs.readdir('.', (error, files) => {
        if (error) throw error;
        for (i in files) {
            if (files[i].endsWith(".csv"))
                fs.unlink(files[i], (err) => {
                    if (err) {
                        console.log("failed to delete local image:" + err);
                    } else {
                        console.log('successfully deleted local image');
                    }
                });
        }

    });
}
//removeCSVfiles();
let con = connectToDB();
make_dir();
getAllTables2(con).then(function (lst) {

    for (let idx in  lst)
        table_names.push(lst[idx].TABLE_NAME);
    populate_quotes();
}).catch(function (err) {
    console.log("ERROR ERROR get created table list " + err)
    return;
});

function isOpenToday() {
    if (testProg) return true;
    var now = moment(new Date()); //todays date

    let day = now.day();
    if (day == 6 || day == 0)
        return false;
    return true;
}
function isOpen(sym) {
    if (testProg) return true;
    var now = moment(new Date()); //todays date
    let hr = now.hour();
    let min = now.minute();
    let day = now.day();
    if (min < 5)
        console.log("top of hour " + hr)
    if (min > 30 && min < 35)
        console.log("mid hour " + hr)
    if (!isOpenToday())
        return false;
    if (hr >= sym.oh && hr <= sym.ch) {
        if (hr == sym.oh && min <= sym.om || hr == sym.ch && min >= sym.cm) {
            return false;
        }
    } else {
        return false;

    }
    return true;
}
function make_dir() {
    var fs = require('fs');

    let dt = moment().format('YYYY-MM-DD');
    if (isOpenToday() && (currentDate == null || currentDate != dt)) {
        currentDate = dt;
        currentPath = "./ftsDump/" + currentDate;
        if (!fs.existsSync(currentPath)) {
            fs.mkdirSync(currentPath);
            for (i in syms) {
                let p = currentPath + "/" + syms[i].s;
                fs.mkdirSync(p);
            }
        }
    }
}
function populate_quotes() {
    setTimeout(populate_quotes, tickSize * 1000);
    tickCnt = tickCnt + 1;
    let dt = moment().format('YYYY-MM-DD');
    let tm = moment().format('HH:mm:ss');
    let quotes_run = false;
    for (i in syms) {
        if (!isOpen(syms[i]))
            continue;
        if (!active) {
            con = connectToDB();
            make_dir();
            return;

        } else {

            quotes_run = true;
            if ( i == 0)
                console.log("new start time =  " + tm);
            let map = [];
            let tablesToCreate = [];
            tbl_cnt = 0;
            getFromAmeritrade(table_names, syms[i].s, 100, map, tablesToCreate, dt, tm);
        }

    }
    if (!quotes_run) {
        active = false;
        if (con)
            con.end();
        con = null;
    }

}
//con.end();
