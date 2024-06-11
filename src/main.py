import time
import re
import bencodepy
import sqlite3
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.internet.task import LoopingCall

class TorrentTracker(resource.Resource):
    isLeaf = True

    def __init__(self, db_path='peers.db'):
        self.cleanup_interval = 1800  # Intervalo para limpeza em segundos
        self.db_path = db_path

        # Conecta ao banco de dados SQLite
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.create_tables()

        # Configura uma chamada periódica para a limpeza de peers
        self.cleanup_task = LoopingCall(self.cleanup_all_peers)
        self.cleanup_task.start(self.cleanup_interval)

    def create_tables(self):
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS peers (
                    info_hash TEXT,
                    peer_id TEXT,
                    ip TEXT,
                    port INTEGER,
                    last_seen REAL,
                    PRIMARY KEY (info_hash, peer_id)
                )
            ''')

    def render_GET(self, request):
        params = {k.decode(): v[0].decode() for k, v in request.args.items()}
        
        # Verifica a validade dos parâmetros
        if 'info_hash' not in params or 'peer_id' not in params or 'port' not in params:
            request.setResponseCode(400)
            return b'Invalid request'

        info_hash = params['info_hash']
        peer_id = params['peer_id']
        port = params['port']
        
        if not re.match(r'^[0-9]+$', port):
            request.setResponseCode(400)
            return b'Invalid port number'

        port = int(port)
        ip = request.getClientIP()

        # Adiciona ou atualiza o peer no banco de dados
        current_time = time.time()
        with self.conn:
            self.conn.execute('''
                INSERT OR REPLACE INTO peers (info_hash, peer_id, ip, port, last_seen)
                VALUES (?, ?, ?, ?, ?)
            ''', (info_hash, peer_id, ip, port, current_time))

        # Limpa peers inativos
        self.cleanup_peers(info_hash, current_time)

        # Prepara a resposta com a lista de peers
        with self.conn:
            cursor = self.conn.execute('''
                SELECT ip, port FROM peers WHERE info_hash = ?
            ''', (info_hash,))
            peers = [{'ip': row[0], 'port': row[1]} for row in cursor]

        response = {
            'interval': self.cleanup_interval,
            'peers': peers
        }
        
        return bencodepy.encode(response)

    def cleanup_all_peers(self):
        current_time = time.time()
        with self.conn:
            self.conn.execute('''
                DELETE FROM peers WHERE ? - last_seen > ?
            ''', (current_time, self.cleanup_interval))

    def cleanup_peers(self, info_hash, current_time):
        with self.conn:
            self.conn.execute('''
                DELETE FROM peers WHERE info_hash = ? AND ? - last_seen > ?
            ''', (info_hash, current_time, self.cleanup_interval))

if __name__ == '__main__':
    tracker = TorrentTracker()
    site = server.Site(tracker)
    reactor.listenTCP(6969, site)
    reactor.run()
