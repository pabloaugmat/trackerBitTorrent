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
                    nome TEXT,
                    tipo_midia TEXT,
                    descricao TEXT,
                    peer_id TEXT,
                    ip TEXT,
                    port INTEGER,
                    last_seen REAL,
                    PRIMARY KEY (info_hash, peer_id)
                )
            ''')

    def render_GET(self, request):

        path = request.path.decode().strip('/').split('/')
        
        if len(path) == 2 and path[1] == 'torrents':
            torrents_data = self.handle_get_all_torrents()
            if torrents_data is None:
                request.setResponseCode(500)
                return b'Failed to fetch torrents data'
            
            bencoded_data = bencodepy.encode(torrents_data)
                
            request.responseHeaders.addRawHeader(b'content-type', b'application/json')
            request.setResponseCode(200)
            return bencoded_data
        elif len(path) == 2 and path[1] == 'update':
            params = {k.decode(): v[0].decode() for k, v in request.args.items()}
            info_hash = params['info_hash']
            peer_id = params['peer_id']
            current_time = time.time()
            with self.conn:
                self.conn.execute('''
                    UPDATE peers SET last_seen = ? WHERE info_hash = ? AND peer_id = ?
                ''', (current_time, info_hash, peer_id))
            return 'Sucesso'
        
        elif len(path) == 2 and path[1] == 'download':
            params = {k.decode(): v[0].decode() for k, v in request.args.items()}
            info_hash = params['info_hash']
            torrents_data = self.handle_get_info_hash(info_hash)
            if torrents_data is None:
                request.setResponseCode(500)
                return b'Failed to fetch torrents data'
            
            bencoded_data = bencodepy.encode(torrents_data)
                
            request.responseHeaders.addRawHeader(b'content-type', b'application/json')
            request.setResponseCode(200)
            return bencoded_data

        
        params = {k.decode(): v[0].decode() for k, v in request.args.items()}
        
        # Verifica a validade dos parâmetros
        if 'info_hash' not in params or 'peer_id' not in params or 'port' not in params:
            request.setResponseCode(400)
            return b'Invalid request'

        info_hash = params['info_hash']
        nome = params['nome']
        tipo_midia = params['tipo_midia']
        descricao = params['descricao']
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
                INSERT OR REPLACE INTO peers (info_hash, nome, tipo_midia, descricao, 
                              peer_id, ip, port, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (info_hash, nome, tipo_midia, descricao, peer_id, ip, port, current_time))

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
    
    def handle_get_all_torrents(self):
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT nome, tipo_midia, descricao, info_hash FROM peers')
                torrents_info = []
                for row in cursor:
                    nome = row[0]
                    tipo_midia = row[1]
                    descricao = row[2]
                    info_hash = row[3]
                    torrents_info.append({
                        'nome': nome,
                        'tipo_midia': tipo_midia,
                        'descricao': descricao,
                        'info_hash': info_hash
                    }) 
                return torrents_info
        except Exception as e:
            print(f"Error fetching torrents from DB: {e}")
            return None
    
    def handle_get_info_hash(self, info_hash):
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT info_hash, peer_id, ip, port FROM peers WHERE info_hash = ?', (info_hash,))
                torrents_info = []
    
                for row in cursor.fetchall():
                    torrents_info.append({
                        'info_hash': row[0],  
                        'peer_id': row[1],    
                        'ip': row[2],        
                        'port': row[3]   
                    })
                return torrents_info
        except Exception as e:
            print(f"Error fetching torrents from DB: {e}")
            return None
    
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
