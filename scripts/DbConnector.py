from sshtunnel import SSHTunnelForwarder
import pymysql
import json


class ConnectionCfg:
    def __init__(self):
        with open('/Users/kevin/PycharmProjects/beactive_datapipeline/configs/cfg.json', 'r') as f:
            config = json.load(f)
        self.PRIVATE_KEY = config['TEST']['PRIVATE_KEY']
        self.HOST = config['TEST']['HOST']
        # self.LOCALHOST = config['TEST']['LOCALHOST']
        self.SSH_USERNAME = config['TEST']['SSH_USERNAME']
        # self.USER = config['TEST']['USER']
        self.PASSWORD = config['TEST']['PASSWORD']
        self.DB = config['TEST']['DB']
        self.REMOTE_BIND_ADDRESS = config['TEST']['REMOTE_BIND_ADDRESS']
        self.REMOTE_PORT = config['TEST']['REMOTE_PORT']


class DbConnector:
    def run(self, db):
        print("Connected" + str(db))

    def connect(self, key=None):
        cfg = ConnectionCfg()
        with SSHTunnelForwarder(
                (cfg.HOST),
                ssh_username=cfg.SSH_USERNAME,
                ssh_pkey=cfg.PRIVATE_KEY,
                remote_bind_address=(cfg.REMOTE_BIND_ADDRESS, cfg.REMOTE_PORT)
        ) as tunnel:
            print("****SSH Tunnel Established****")
            db = pymysql.connect(
                host='127.0.0.1', user="admin",
                password=cfg.PASSWORD, port=tunnel.local_bind_port
            )
            try:
                self.run(db)

            except Exception as e:
                print("Run Failed", e)

            finally:
                db.close()
                del tunnel
