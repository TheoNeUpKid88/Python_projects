import os
import sys
import sqlite3
from sqlite3 import Error

class autoDB:

    def __init__(self, dataLocation=os.curdir + "/" + 'pythonsqlite.db'):

        projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                                id integer PRIMARY KEY,
                                                name text NOT NULL,
                                                begin_date text,
                                                end_date text
                                            ); """
        tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                            id integer PRIMARY KEY,
                                            name text NOT NULL,
                                            priority integer,
                                            status_id integer NOT NULL,
                                            project_id integer NOT NULL,
                                            begin_date text NOT NULL,
                                            end_date text NOT NULL,
                                            FOREIGN KEY (project_id) REFERENCES projects (id)
                                        );"""
        Niches_table = """ CREATE TABLE IF NOT EXISTS niches (
                                                        id integer PRIMARY KEY,
                                                        name text NOT NULL,
                                                        begin_date text,
                                                        end_date text
                                                    ); """
        Market_table = """CREATE TABLE IF NOT EXISTS market (
                                                    id integer PRIMARY KEY,
                                                    name text NOT NULL,
                                                    priority integer,
                                                    status_id integer NOT NULL,
                                                    project_id integer NOT NULL,
                                                    begin_date text NOT NULL,
                                                    end_date text NOT NULL,
                                                    FOREIGN KEY (project_id) REFERENCES niches (id)
                                                );"""
        SubNiche_table = """CREATE TABLE IF NOT EXISTS subniche (
                                                            id integer PRIMARY KEY,
                                                            name text NOT NULL,
                                                            priority integer,
                                                            status_id integer NOT NULL,
                                                            project_id integer NOT NULL,
                                                            begin_date text NOT NULL,
                                                            end_date text NOT NULL,
                                                            FOREIGN KEY (project_id) REFERENCES niches (id)
                                                        );"""

        self.database = dataLocation
        self.conn = self.create_connection(self.database)

    def create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        try:
            conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)
        finally:
            conn.close()
        self.database = db_file
        return conn

    def close_connection(self):
        """
        close database connection
        :return:
        """
        return self.conn.close()

    def create_table(self, create_table_sql):
        """ create a table from the create_table_sql statement
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """

        if self.conn is not None:
            try:
                c = self.conn.cursor()
                c.execute(create_table_sql)
            except Error as e:
                print(e)
        else:
            sys.exit('SQLITE conn not established ')

    def create_project(self, project):
        """
        Create a new project into the projects table
        :param project: tuple - name,begin_date,end_date
        :return: project id
        """
        if self.conn is not None:
            sql = ''' INSERT INTO projects(name,begin_date,end_date)
                          VALUES(?,?,?) '''
            cur = self.conn.cursor()
            cur.execute(sql, project)
            return cur.lastrowid
        else:
            sys.exit('SQLITE conn not established ')

    def inserting_into_Table(self, table, task):
        """
        Insert data into table
        :param table: string
        :param task: tuple - name,priority,status_id,project_id,begin_date,end_date:
        :return:
        """
        if self.conn is not None:
            sql = ''' INSERT INTO {0}(name,priority, status_id, project_id, begin_date, end_date)
                          VALUES(?,?,?,?,?,?) '''.format(table)

            cur = self.conn.cursor()
            cur.execute(sql, task)
            return cur.lastrowid
        else:
            sys.exit('SQLITE conn not established ')

    def update_Table(self, table, data):
        """
        update priority, begin_date, and end date of a task
        :param table: table name
        :param data: tuple - name,priority,status_id,project_id,begin_date,end_date:
        :return: project id
        """

        if self.conn is not None:
            sql = ''' UPDATE {0}
                      SET priority = ? ,
                          begin_date = ? ,
                          end_date = ?
                      WHERE id = ?'''.format(table)

            cur = self.conn.cursor()
            return cur.execute(sql, data)
        else:
            sys.exit('SQLITE conn not established ...')

    def delete_Records(self, table, _id):
        """
        Delete a task by task id
        :param table: name of table
        :param _id: id of the task
        :return:
        """
        if self.conn is not None:
            sql = 'DELETE FROM {} WHERE id=?'.format(table)
            cur = self.conn.cursor()
            cur.execute(sql, (_id,))
        else:
            sys.exit('SQLITE conn not established ...')

    def delete_all_Table_Records(self, table):
        """
        Delete all rows in the table
        :param table: name of table
        :return:
        """
        if self.conn is not None:
            sql = 'DELETE FROM {}'.format(table)
            cur = self.conn.cursor()
            return cur.execute(sql)
        else:
            sys.exit('SQLITE conn not established ...')

    def select_all_tableData(self, table):
        """
        Query all rows in table
        :param table: name of table
        :return:
        """
        if self.conn is not None:
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM " + table)

            rows = cur.fetchall()
            data = []
            for row in rows:
                data.append(row)
            return data
        else:
            sys.exit('SQLITE conn not established ...')

    def select_data_from_table_by_priority(self, table, priority):
        """
        Query tasks by priority
        :param table: name of table:
        :param priority: number _id:
        :return:
        """
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM {} WHERE priority=?".format(table), (priority,))

        rows = cur.fetchall()
        data = []
        for row in rows:
            data.append(row)
        return data