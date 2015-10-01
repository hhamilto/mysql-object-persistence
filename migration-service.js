_ = require('lodash')
deferred = require('deferred')
async = require('async')

path = require('path')

//dropAndCreateSql = fs.readFileSync(__dirname+'/dropAndCreate.sql').toString()

module.exports.runMigrations = function(conf){
	var dfd = deferred()	
	fs.readdir(conf.migrationDir, function(err, availableMigrations){
		if(err) {
			dfd.reject(err)
			return
		}
		var migrationConn = mysql.createConnection(_.defaults({
			multipleStatements: true
		},conf.database))
		createMigrationTableIfNotExist(migrationConn,conf).done(function(){
			migrationConn.query('SELECT fileName FROM Migrations', function(err, rows){
				if(err) {
					dfd.reject(err)
					return
				}
				var appliedMigrations = _.map(rows,'fileName')
				var unappliedMigrations = _.difference(availableMigrations, appliedMigrations)
				async.each(unappliedMigrations, function(migrationFileName, cb){
					fs.readFile(path.join(conf.migrationDir,migrationFileName), function(err, sqlContent){
						if(err) cb(err)
						else migrationConn.query(sqlContent.toString(), function(err){
							if(err) cb(err)
							else migrationConn.query('INSERT INTO Migrations (fileName) VALUES (?)', migrationFileName, function(err){
								if(err) cb(err)
								else cb()
							})
						})
					})
				}, function(err){
					if (err) dfd.reject(err)
					else migrationConn.end(function(err){ 
						if (err) dfd.reject(err)
						else dfd.resolve()
					})
				})
			})
		})
	})

	
	return dfd.promise
}

createMigrationTableIfNotExist = function(migrationConn,conf){
	var dfd = deferred()
		migrationConn.query('SHOW TABLES', function(err, results) {
			if (err) {
				dfd.reject(err)
				return
			}
			var filterObj = {}
			filterObj['Tables_in_'+conf.database.database] = 'Migrations'
			if(_.filter(results, filterObj).length == 0){
				fs.readFile(path.join(__dirname,'createMigrationTable.sql'), function(err, createMigrationTableSql){
					if (err) {
						dfd.reject(err)
						return
					}
					migrationConn.query(createMigrationTableSql.toString(), function(err, results) {
						if (err) dfd.reject(err)
						else dfd.resolve()
					})
				})
				
			}else{
				dfd.resolve()
			}
		})	
	

	return dfd.promise
}

module.exports.dropAndCreate = function(){
	var dfd = deferred()
	migrationConn.query(dropAndCreateSql, function(err, results) {
		if (err) dfd.reject(err)
		else dfd.resolve()
	})
	return dfd.promise
}

