# -*- coding: utf-8 -*-
"""
    File:    TransaqReplayServer.py
    Author:  Daniil Dolbilov
    Created: 18-Oct-2020
"""

import logging
import threading
import time
from multiprocessing.connection import Listener


class TransaqReplayServer:

    def __init__(self, xdf_file, host='localhost', port=7070, authkey=b'secret phrase'):
        self.xdf_file = xdf_file
        self.xdf_encoding = 'windows-1251'
        self.skip_list = ['<pits>', '<securities>', '<sec_info_upd>']

        self.host = host
        self.port = port
        self.authkey = authkey

    def run_server(self):
        logging.info('server: listen [%s:%s]...' % (self.host, self.port))
        with Listener((self.host, self.port), authkey=self.authkey) as listener:
            logging.info('server: listen started')
            while True:
                conn = listener.accept()
                logging.info('server: client accepted [%s]' % conn._handle)

                c_thread = threading.Thread(target=self.client_thread, args=(conn, ))
                c_thread.start()

    def client_thread(self, conn):
        self.replay_events(conn)
        logging.info('server: close [%s]' % conn._handle)
        conn.close()

    def replay_events(self, conn):
        with open(self.xdf_file, mode='r', encoding=self.xdf_encoding) as f:
            for line in f:
                # === FORMAT SAMPLE ===
                # 140551.705448 [4804] [0360] <cmd> [V] System version 6.06. TXmlConnector version 2.20.25
                # 140551.718466 [4804] [0360] <cmd> [I] <command id="connect"><login> ...
                # 140552.720939 [4804] [0360] <res> [R] <result success="true"/>
                # 140553.019380 [4804] [clbk] <info> [O] [830u] <markets> ...
                # 140553.353871 [4804] [clbk] <info> [O] [150770u] <securities><security secid="0" active="true">  ...
                # 140605.644962 [4804] [clbk] <info> [O] [333u] <sec_info_upd><secid>29244</secid><seccode>BR55BJ0</seccode> ...
                # 140605.645964 [4804] [clbk] <info> [O] [338u] <sec_info_upd><secid>29245</secid><seccode>BR55BV0</seccode> ...
                # 140624.969489 [4804] [clbk] <info> [O] [4304u] <orders><order transactionid="195726"> ...
                # 140624.973494 [4804] [clbk] <info> [O] [3540u] <trades><trade><secid>41824</secid> ...
                # 140624.980504 [4804] [clbk] <info> [O] [6139u] <positions><forts_position><client> ...
                # 140643.576875 [4804] [clbk] <info> [O] [861u] <quotations><quotation secid="32518"><board>FUT</board><seccode>SiZ0</seccode><last>78508</last><quantity>12</quantity> ...
                # 140643.676021 [4804] [clbk] <info> [O] [862u] <quotations><quotation secid="32518"><board>FUT</board><seccode>SiZ0</seccode><last>78509</last><quantity>1</quantity> ...
                ss = line.split(' ')
                if len(ss) < 7 or ss[4] != '[O]':
                    logging.warning('SKIP: %s' % line.strip())
                    continue

                header = ' '.join(ss[0:6])
                xml_msg = line[len(header) + 1:].strip()

                if xml_msg.startswith(tuple(self.skip_list)):
                    continue

                # TODO: emulate pause between events (historical)
                time.sleep(0.5)

                msg_trimmed = xml_msg[:50] # data is very long, just log some chunk
                if conn:
                    logging.debug('[%s] send: %s ...' % (conn._handle, msg_trimmed))
                    conn.send(xml_msg)
                else:
                    logging.debug('no_conn: %s ...' % msg_trimmed)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s')

    xdf_file = '../logs/2020.10.16-140551/20201016_xdf.log'
    replayer = TransaqReplayServer(xdf_file)
    replayer.run_server()
