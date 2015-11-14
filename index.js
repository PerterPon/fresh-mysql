"use strict";
var BEGIN, COMMIT, Db, RESET, ROLLBACK, db, dbOpt, log, mysql, os, pipe,
  slice = [].slice;

mysql = null;

os = require('options-stream');

log = null;

pipe = require('event-pipe');

BEGIN = "SET autocommit = 0;";

RESET = "SET autocommit = 1;";

COMMIT = "COMMIT;";

ROLLBACK = "ROLLBACK;";

dbOpt = {
  waitForConnections: true
};

db = null;

Db = (function() {
  function Db(type) {
    this.type = type;
    this.db = null;
    this.pool = null;
  }

  Db.prototype.init = function(options, log) {
    if ( void( 0 ) == options ) {
      options = {};
    }
    var database, dbCfg, host, password, port, showLog, user;
    this.mysql = options.mysql || require('mysql');
    host = options.host, port = options.port, user = options.user, password = options.password, database = options.database, showLog = options.showLog;
    if (log == null) {
      log = console;
    }
    if (false === showLog) {
      this.log = {
        info: function() {},
        warn: function() {},
        error: function() {}
      };
    } else {
      this.log = log;
    }
    dbCfg = {
      host: host,
      port: port,
      user: user,
      password: password,
      database: database
    };
    dbCfg = os(dbCfg, dbOpt);
    return this._initConnectPool(dbCfg);
  };

  Db.prototype._initConnectPool = function(dbCfg) {
    log = this.log;
    log.info({
      action: 'MYSQL_INFO',
      info: 'create mysql connection pool'
    });
    this.pool = this.mysql.createPool(dbCfg);
    return this.pool.on('error', (function(_this) {
      return function(err) {
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
          log.warn({
            action: 'MYSQL_WANING',
            info: 'mysql connection lost and try to reconnect mysql server'
          });
          return _this._initConnectPool(dbCfg);
        } else {
          log.error({
            action: 'MYSQL_ERROR',
            info: "db error:" + (JSON.stringify(err))
          });
          throw err;
        }
      };
    })(this));
  };

  Db.prototype.escapeQuery = function(query, values) {
    return {
      sql: query
    };
  };

  Db.prototype.query = function(sql, where, cb) {
    return this._wrapQuery(sql, where, cb);
  };

  Db.prototype.transactionQuery = function(sqlList, callback) {
    var connection, ep, fn, i, item, len, options, that;
    ep = pipe();
    that = this;
    log = this.log;
    connection = null;
    options = {};
    ep.on('error', function(errors) {
      log.error({
        action: 'MYSQL_ERROR',
        info: this.container.stepMessage + " with error:\"" + (JSON.stringify(errors)) + "\""
      });
      return connection.query(ROLLBACK, function() {
        log.warn({
          action: 'MYSQL_TRANSACTION_QUERY',
          info: 'transaction rollback!'
        });
        connection.release();
        return connection.query(RESET, function(err) {
          if (err) {
            connection.destroy();
          }
          return callback(errors);
        });
      });
    });
    ep.lazy(function() {
      this.stepMessage = 'get db connection';
      return that._getConnection(this);
    });
    ep.lazy(function(connect) {
      connection = connect;
      options.connection = connect;
      return this();
    });
    ep.lazy(function() {
      this.stepMessage = 'start transaction';
      return connection.query(BEGIN, this);
    });
    fn = function(item) {
      var argItem, cb, sql, where;
      if (Array.isArray(item)) {
        ep.lazy((function() {
          var j, len1, results;
          results = [];
          for (j = 0, len1 = item.length; j < len1; j++) {
            argItem = item[j];
            sql = argItem.sql, where = argItem.where;
            results.push((function(sql, where) {
              return function() {
                this.stepMessage = 'run sql';
                return that._wrapQuery(sql, where, this, options);
              };
            })(sql, where));
          }
          return results;
        })());
        return ep.lazy(function() {
          var args, cb, e, error, error1, index, j, len1, results;
          args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
          results = [];
          for (index = j = 0, len1 = item.length; j < len1; index = ++j) {
            argItem = item[index];
            cb = argItem.cb;
            error = null;
            try {
              if (cb != null) {
                cb.apply(null, args[index]);
              }
            } catch (error1) {
              e = error1;
              error = {
                message: e.message
              };
            }
            results.push(this(error));
          }
          return results;
        });
      } else {
        sql = item.sql, where = item.where, cb = item.cb;
        ep.lazy(function() {
          this.stepMessage = 'run sql';
          return that._wrapQuery(sql, where, this, options);
        });
        return ep.lazy(function() {
          var args, e, error, error1;
          args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
          error = null;
          try {
            if (cb != null) {
              cb.apply(null, args);
            }
          } catch (error1) {
            e = error1;
            error = {
              message: e.message
            };
          }
          return this(error);
        });
      }
    };
    for (i = 0, len = sqlList.length; i < len; i++) {
      item = sqlList[i];
      fn(item);
    }
    ep.lazy(function() {
      this.stepMessage = 'commit';
      return connection.query(COMMIT, this);
    });
    ep.lazy(function() {
      log.info({
        action: 'MYSQL_TRANSACTION_QUERY',
        info: 'commit success!'
      });
      connection.release();
      callback(null);
      return connection.query(RESET, function(err) {
        if (err) {
          return connection.destroy();
        }
      });
    });
    return ep.run();
  };

  Db.prototype._getConnection = function(cb, retry) {
    var that;
    if (retry == null) {
      retry = 0;
    }
    that = this;
    log = this.log;
    return this.pool.getConnection(function(err, connection) {
      if (err) {
        log.error({
          action: 'MYSQL_ERROR',
          info: "get connection error:" + (JSON.stringify(err))
        });
        if (retry++ >= 2) {
          log.error({
            action: 'MYSQL_ERROR',
            info: "get connection error after retry 3 times:" + (JSON.stringify(err))
          });
          return cb(err, null);
        } else {
          return setTimeout(function() {
            return that._getConnection(cb, retry);
          }, 200);
        }
      } else {
        return cb(null, connection);
      }
    });
  };

  Db.prototype._wrapQuery = function(sql, where, cb, options) {
    var autoRelease, connection, retry, that;
    if (options == null) {
      options = {};
    }
    if (options.autoRelease == null) {
      options.autoRelease = true;
    }
    if (options.retry == null) {
      options.retry = 0;
    }
    retry = options.retry, autoRelease = options.autoRelease, connection = options.connection;
    that = this;
    if (connection) {
      options.autoRelease = false;
      return this._doQuery(sql, where, cb, options);
    } else {
      return this._getConnection((function(_this) {
        return function(err, connection) {
          if (err) {
            return cb(err);
          }
          options.connection = connection;
          return _this._doQuery(sql, where, cb, options);
        };
      })(this));
    }
  };

  Db.prototype._doQuery = function(sql, where, cb, options) {
    var autoRelease, connection, escapedSql;
    mysql = this.mysql, log = this.log;
    autoRelease = options.autoRelease, connection = options.connection;
    escapedSql = this.escapeQuery(sql, where);
    sql = escapedSql.sql;
    return (function(sql, connection, autoRelease, cb) {
      sql = mysql.format(sql, where);
      log.info({
        action: 'MYSQL_QUERY_REQUEST',
        info: sql
      });
      return connection.query(sql, function(err, data) {
        var errMsg, errStack;
        if (autoRelease === true) {
          connection.release();
        }
        if (err) {
          errMsg = err.message;
          errStack = err.stack;
          log.error({
            action: 'MYSQL_QUERY_ERROR',
            info: JSON.stringify({
              errMsg: errMsg,
              sql: sql
            })
          });
        } else {
          log.info({
            action: 'MYSQL_QUERY_RESPONSE',
            info: JSON.stringify({
              data: data
            })
          });
        }
        return cb(err, data);
      });
    })(sql, connection, autoRelease, cb);
  };

  Db.prototype.escape = function() {
    var args;
    args = 1 <= arguments.length ? slice.call(arguments, 0) : [];
    return this.pool.escape.apply(this.pool, args);
  };

  Db.prototype.destroy = function() {
    this.db = null;
    if (this.pool) {
      return this.pool.end();
    }
  };

  return Db;

})();

module.exports = Db;