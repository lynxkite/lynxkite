import unittest
import lynx
import os

class TestImportTest(unittest.TestCase):
    """
    json, orc, or parquet call the same functions as csv, so they are not
    same goes for jdbc and hive
    """
    def test_csv_http(self):
        """needs python -m SImpleHTTPServer running in test directory
        i couldn't figure out a way to do this reliably, see attempts below
        """
        port = 5232
        import lynx
        server = StoppableHTTPServer(port)
        p = lynx.Project()
        table = p.importCSV(
            files="http://localhost:{}/tests/import_csv_test.csv".format(port), table="http import")
        server.stop()

    def test_csv_local(self):
        p = lynx.Project()
        table = p.importCSV(
            files = os.path.abspath("tests/import_csv_test.csv"),
            table = "new table")

    def test_jdbc(self):
        import sqlite3
        path = os.path.abspath("tests/test.db")
        conn = sqlite3.connect(path)
        c = conn.cursor()
        c.executescript("""
        DROP TABLE IF EXISTS subscribers;
        CREATE TABLE subscribers
        (n TEXT, id INTEGER, name TEXT, gender TEXT, "race condition" TEXT, level DOUBLE PRECISION);
        INSERT INTO subscribers VALUES
        ('A', 1, 'Daniel', 'Male', 'Halfling', 10.0),
        ('B', 2, 'Beata', 'Female', 'Dwarf', 20.0),
        ('C', 3, 'Felix', 'Male', 'Gnome', NULL),
        (NULL, 4, NULL, NULL, NULL, NULL);
        """)
        conn.commit()
        conn.close()

        url = "jdbc:sqlite:{}".format(path)
        p = lynx.Project()
        p.importJdbc(table = "jdbc-import", jdbcUrl=url, jdbcTable="subscribers", keyColumn="id")


import http.server
import socketserver
import threading
class StoppableHTTPServer():
    def __init__(self, port):
        Handler = http.server.SimpleHTTPRequestHandler
        httpd = socketserver.TCPServer(('127.0.0.1', port), Handler, False)
        httpd.allow_reuse_address = True
        httpd.server_bind()
        httpd.server_activate()
        thread = threading.Thread(target = httpd.serve_forever)
        thread.start()
        self.thread = thread
        self.httpd = httpd
    def run(self):
        try:
            self.serve_forever(poll_interval=0.01)
        except KeyboardInterrupt:
            pass
        finally:
            # Clean-up server (close socket, etc.)
            self.server_close()

    def stop(self):
        self.httpd.socket.close()
        self.httpd.shutdown()
        self.thread.join()


if __name__ == '__main__':
    unittest.main()
