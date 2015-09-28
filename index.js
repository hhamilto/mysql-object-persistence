mysql = require('mysql')
_ = require('lodash')
deferred = require('deferred')

fs = require('fs')
util = require('util')


whereClauseFromObject = function(obj){
	return _.map(_.pairs(obj), function(pair){
		return mysql.escapeId(pair[0]) + '=' + mysql.escape(pair[1])
	}).join(' AND ')
}

migrationService = require('./migration-service.js')

var dbReady = false


module.exports.initialize =function(conf){
	if(dbReady) return dbReady
	dbReady = deferred()

	migrationService.runMigrations(conf).done(function(){
		var conn = mysql.createConnection(conf.database)
		var ignoredTables = ['Migrations']
		module.exports.close = conn.end.bind(conn)

		conn.query('SHOW TABLES', function(err,rows){
			if(err) {
				dbReady.reject(err)
				return
			}
			var tables = _.without(_.map(rows,'Tables_in_'+conf.database.database), 'Migrations')
			deferred.map(tables, function(table){
				var dfd = deferred()
				module.exports[table] = {}
				conn.query('SHOW COLUMNS FROM ' + table, function(err,rows){
					if(err){
						dfd.reject(err)
						return
					}
					module.exports[table].columns = rows
					dfd.resolve()
				})
				module.exports[table].create = function(newObj){
					dfd = deferred()
					conn.query('INSERT INTO '+table+' SET ?', newObj, function(err, result){
						if(err)dfd.reject(err)
						else dfd.resolve(result.insertId)
					})
					return dfd.promise
				}
				module.exports[table].get = function(obj){
					dfd = deferred()
					whereClause = _.keys(obj).length>0?' WHERE '+ whereClauseFromObject(obj):''
					conn.query('SELECT * FROM '+table+whereClause, function(err, rows){
						if(err)dfd.reject(err)
						else dfd.resolve(rows)
					})
					return dfd.promise
					
				}
				module.exports[table].set = function(newObj){
					var id = newObj.id;
					delete newObj.id;
					dfd = deferred()
					conn.query('UPDATE '+table+' SET ? WHERE id=?', [newObj, id], function(err, result){
						if(err)dfd.reject(err)
						else dfd.resolve(result.insertId)
					})
					return dfd.promise
				}
				module.exports[table].remove = function(obj){
					dfd = deferred()
					conn.query('DELETE FROM '+table+' WHERE '+whereClauseFromObject(obj), function(err, result){
						if(err)dfd.reject(err)
						else dfd.resolve()
					})
					return dfd.promise
				}
				return dfd
			}).then(dbReady.resolve,dbReady.reject)
		})
		
	})
	return dbReady.promise
}


