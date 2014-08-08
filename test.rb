
require 'sqlite3'

  db = SQLite3::Database.new( "db/schedule.db" )
  rows = db.execute( "select * from schedule" )
  db.close()
