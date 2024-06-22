import time
import re
import bencodepy
import sqlite3
import logging
from urllib.parse import urlparse, parse_qs
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.internet.task import LoopingCall

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TorrentTracker(resource.Resource):
    isLeaf = True

    def __init__(self, db_path='peers.db'):
        super().__init__()
        self.cleanup_interval = 1800  # Intervalo para limpeza em segundos
        self.db_path = db_path

        # Conecta ao banco de dados SQLite
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.create_tables()

        # Configura uma chamada periódica para a limpeza de peers
        self.cleanup_task = LoopingCall(self.cleanup_all_peers)
        self.cleanup_task.start(self.cleanup_interval)

        logger.info("Torrent Tracker iniciado.")

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
        logger.info("Tabela 'peers' criada ou verificada.")

    def cleanup_all_peers(self):
        current_time = time.time()
        with self.conn:
            self.conn.execute('''
                DELETE FROM peers WHERE ? - last_seen > ?
            ''', (current_time, self.cleanup_interval))
        logger.info("Peers inativos limpos.")

    def cleanup_peers(self, info_hash, current_time):
        with self.conn:
            self.conn.execute('''
                DELETE FROM peers WHERE info_hash = ? AND ? - last_seen > ?
            ''', (info_hash, current_time, self.cleanup_interval))
        logger.info(f"Peers inativos limpos para o info_hash {info_hash}.")

    def render_GET(self, request):
        path = request.path.decode().strip('/').split('/')
        logger.info(f"Requisição recebida: {request.path.decode()}")

        if len(path) == 2 and path[1] == 'torrents':
            torrents_data = self.handle_get_all_torrents()
            if torrents_data is None:
                request.setResponseCode(500)
                return b'Failed to fetch torrents data'
            
            bencoded_data = bencodepy.encode(torrents_data)
            logger.info("Dados dos torrents recuperados com sucesso.")
                
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
            logger.info(f"Peer {peer_id} atualizado com sucesso para o info_hash {info_hash}.")
            return b'Success'
        elif len(path) == 2 and path[1] == 'download':
            params = {k.decode(): v[0].decode() for k, v in request.args.items()}
            info_hash = params['info_hash']
            torrents_data = self.handle_get_info_hash(info_hash)
            if torrents_data is None:
                request.setResponseCode(500)
                return b'Failed to fetch torrents data'
            
            bencoded_data = bencodepy.encode(torrents_data)
            logger.info(f"Dados dos torrents para {info_hash} recuperados com sucesso.")
                
            request.responseHeaders.addRawHeader(b'content-type', b'application/json')
            request.setResponseCode(200)
            return bencoded_data

        # Parâmetros obrigatórios para o announce do BitTorrent
        params = {k.decode(): v[0].decode() for k, v in request.args.items()}
        
        if 'info_hash' not in params or 'peer_id' not in params or 'port' not in params:
            request.setResponseCode(400)
            logger.warning("Requisição inválida recebida. Parâmetros faltando.")
            return b'Invalid request'

        info_hash = params['info_hash']
        nome = params.get('nome', '')
        tipo_midia = params.get('tipo_midia', '')
        descricao = params.get('descricao', '')
        peer_id = params['peer_id']
        port = params['port']

        if not re.match(r'^[0-9]+$', port):
            request.setResponseCode(400)
            logger.warning(f"Número de porta inválido: {port}")
            return b'Invalid port number'

        port = int(port)
        ip = request.getClientIP()

        # Adiciona ou atualiza o peer no banco de dados
        current_time = time.time()
        try:
            with self.conn:
                self.conn.execute('''
                    INSERT OR REPLACE INTO peers (info_hash, nome, tipo_midia, descricao, 
                                  peer_id, ip, port, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (info_hash, nome, tipo_midia, descricao, peer_id, ip, port, current_time))
            logger.info(f"Peer {peer_id} adicionado/atualizado com sucesso para o info_hash {info_hash}.")
        except Exception as e:
            logger.error(f"Erro ao inserir/atualizar peer: {e}")
            request.setResponseCode(500)
            return b'Failed to update peer'

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

        encoded_response = bencodepy.encode(response)
        logger.info(f"Resposta preparada com {len(peers)} peers para o info_hash {info_hash}.")

        request.setHeader(b'content-type', b'application/x-bittorrent-tracker')
        request.setHeader(b'content-length', str(len(encoded_response)).encode('utf-8'))
        request.write(encoded_response)
        request.finish()

        return server.NOT_DONE_YET

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
                logger.info(f"{len(torrents_info)} torrents recuperados do banco de dados.")
                return torrents_info
        except Exception as e:
            logger.error(f"Erro ao buscar torrents no banco de dados: {e}")
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
                logger.info(f"{len(torrents_info)} peers recuperados para o info_hash {info_hash}.")
                return torrents_info
        except Exception as e:
            logger.error(f"Erro ao buscar torrents no banco de dados: {e}")
            return None

if __name__ == '__main__':
    tracker = TorrentTracker()
    site = server.Site(tracker)
    reactor.listenTCP(6969, site)
    logger.info("Servidor escutando na porta 6969.")
    reactor.run()
