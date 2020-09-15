from sshtunnel import SSHTunnelForwarder
from scripts.DbConnector import ConnectionCfg
import pandas as pd
import pymysql

'''
SSH Connect to AWS RDB
User SQL to get DB info and Table data
Convert Table data to pd.DataFrame
'''


def send_query(sql):
    cfg = ConnectionCfg()

    with SSHTunnelForwarder(
            cfg.HOST,
            ssh_username=cfg.SSH_USERNAME,
            ssh_pkey=cfg.PRIVATE_KEY,
            remote_bind_address=(cfg.REMOTE_BIND_ADDRESS, cfg.REMOTE_PORT)
    ) as tunnel:
        print("****SSH Tunnel Established****")

        db = pymysql.connect(
            host='127.0.0.1', user="admin",
            password=cfg.PASSWORD, port=tunnel.local_bind_port
        )
        # Run sample query in the database to validate connection
        result = []
        try:
            # Print all the databases
            cur = db.cursor()
            cur.execute('USE ' + cfg.DB)
            cur.execute(sql)
            for r in cur:
                result.append(r)

        finally:
            db.close()
            del tunnel
            return result


def send_query_multi(sqls):
    cfg = ConnectionCfg()
    with SSHTunnelForwarder(
            cfg.HOST,
            ssh_username=cfg.SSH_USERNAME,
            ssh_pkey=cfg.PRIVATE_KEY,
            remote_bind_address=(cfg.REMOTE_BIND_ADDRESS, cfg.REMOTE_PORT)
    ) as tunnel:
        print("****SSH Tunnel Established****")

        db = pymysql.connect(
            host='127.0.0.1', user="admin",
            password=cfg.PASSWORD, port=tunnel.local_bind_port
        )
        # Run sample query in the database to validate connection
        try:
            # Print all the databases
            result = []
            cur = db.cursor()
            cur.execute('USE ' + cfg.DB)
            for sql in sqls:
                cur.execute(sql)
                for r in cur:
                    result.append(r)
                    print(r)

        finally:
            db.close()
            del tunnel
            return result


def multi_query(sql, ids: list):
    cfg = ConnectionCfg()
    with SSHTunnelForwarder(
            cfg.HOST,
            ssh_username=cfg.SSH_USERNAME,
            ssh_pkey=cfg.PRIVATE_KEY,
            remote_bind_address=(cfg.REMOTE_BIND_ADDRESS, cfg.REMOTE_PORT)
    ) as tunnel:
        print("****SSH Tunnel Established****")

        db = pymysql.connect(
            host='127.0.0.1', user="admin",
            password=cfg.PASSWORD, port=tunnel.local_bind_port
        )

        result = {}
        try:
            cur = db.cursor()
            cur.execute('USE ' + cfg.DB)
            for id in ids:
                rows = []
                cur.execute(sql, (id))
                for r in cur:
                    rows.append(r)
                result[id] = pd.DataFrame(rows)
        finally:
            db.close()
            del tunnel
            return result
