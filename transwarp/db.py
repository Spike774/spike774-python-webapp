# db.py
'''
transwarp.db module is created to achieve functions below:
1, initialize database connection info - create_engine()
2, a method to query, return a list with dict as keys - select()
3, to execute INSERT, UPDATE and DELETE with auto closing - update(sql, *args)
'''

import os, re, sys, time, uuid, socket, datetime, functools, threading, logging, collections

# database engine object, global vars
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect
    def connect(self):
        return self._connect

engine = None

# the object containing up and down stream of the database, global vars
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_clean = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_clean = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

'''
with connection():
    do_some_db_operation()
---------------------------------
@with_connection
def do_some_db_operation():
    pass
'''

class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		return self
		
	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions == 0
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()
				
	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except:
			_db_ctx.connection.rollback()
			raise
			
	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()
		
def select():
	pass
	
def update():
	pass