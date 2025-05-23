import sqlite3

con = sqlite3.connect("shipment_database.db")
cur = con.cursor()

cur.execute("DELETE FROM product")
cur.execute("DELETE FROM shipment")


con.commit()
con.close()