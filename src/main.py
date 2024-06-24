import time
import bencodepy
import sqlite3
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.internet.task import LoopingCall

class Tracker(resource.Resource):
    isLeaf = True #recurso folha
    peers = {} #dict: chave é info_hash e o valor é uma lista tuplas contento informações dos peers

    def render_GET(self, request):
        # parâmetros de consulta
        info_hash = request.args[b'info_hash'][0]
        peer_id = request.args[b'peer_id'][0]
        ip = request.getClientIP().encode('utf-8')
        port = request.args[b'port'][0]
        event = request.args.get(b'event', [b''])[0]

        # Log request para debug
        print(f"Received request: info_hash={info_hash}, peer_id={peer_id}, ip={ip}, port={port}, event={event}")

        # Inicializa a lista de peers para info_hash
        if info_hash not in self.peers:
            self.peers[info_hash] = []

        # Condionais para lidar com eventos diferentes
        if event == b'started':
            self.peers[info_hash].append((peer_id, ip, port, time.time()))
        elif event == b'stopped':
            self.peers[info_hash] = [p for p in self.peers[info_hash] if p[0] != peer_id]
        elif event == b'completed':
            # opcional
            pass
        else:
            # Update timestamp para regular announces
            for peer in self.peers[info_hash]:
                if peer[0] == peer_id:
                    self.peers[info_hash].remove(peer)
                    self.peers[info_hash].append((peer_id, ip, port, time.time()))

        # Remove peers obsoletos
        current_time = time.time()
        self.peers[info_hash] = [p for p in self.peers[info_hash] if current_time - p[3] < 1800]

        # Resposta da requição
        response = b'd8:intervali1800e5:peers'
        peers_list = b''.join([
            ip + int(port).to_bytes(2, 'big') for _, ip, port, _ in self.peers[info_hash]
        ])
        response += str(len(peers_list)).encode('utf-8') + b':' + peers_list + b'e'

        print(f"Response: ", response)
        return response

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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT,
                    tipo_midia TEXT,
                    descricao TEXT,
                    link_magnetico TEXT,
                    last_seen REAL
                )
            ''')

    def render_GET(self, request):
        path = request.path.decode().strip('/').split('/') # caminho da requisição
        
        if len(path) == 2 and path[1] == 'torrents':
            torrents_data = self.handle_get_all_torrents()
            if torrents_data is None:
                request.setResponseCode(500)
                return b'Falha ao buscar dados de torrents'
            
            bencoded_data = bencodepy.encode(torrents_data)
            request.responseHeaders.addRawHeader(b'content-type', b'application/json')
            request.setResponseCode(200)
            return bencoded_data
        
        try:
            params = {k.decode(): v[0].decode() for k, v in request.args.items()}
            
            nome = params['nome']
            tipo_midia = params['tipo_midia']
            descricao = params['descricao']
            link_magnetico = params['link_magnetico']
            print("nome:", nome)
            print("tipo midia:", tipo_midia)
            print("descricao:", descricao)
            print("link magnetico:", link_magnetico)
            
            current_time = time.time()
            with self.conn:
                self.conn.execute('''
                    INSERT OR REPLACE INTO peers (nome, tipo_midia, descricao, link_magnetico, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                ''', (nome, tipo_midia, descricao, link_magnetico, current_time))
                
            request.setResponseCode(200)
            return b'Adicionados com sucesso os dados do torrent'
        
        except KeyError as e:
            request.setResponseCode(400)
            return f'Missing parameter: {str(e)}'.encode('utf-8')
        
        except Exception as e:
            request.setResponseCode(500)
            return f'Internal server error: {str(e)}'.encode('utf-8')

    def handle_get_all_torrents(self):
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT nome, tipo_midia, descricao, link_magnetico FROM peers')
                torrents_info = []
                for row in cursor:
                    nome = row[0]
                    tipo_midia = row[1]
                    descricao = row[2]
                    link_magnetico = row[3]
                    torrents_info.append({
                        'nome': nome,
                        'tipo_midia': tipo_midia,
                        'descricao': descricao,
                        'link_magnetico': link_magnetico
                    }) 
                return torrents_info
        except Exception as e:
            print(f"Erro ao buscar torrents do banco de dados: {e}")
            return None
        
    def cleanup_all_peers(self):
        current_time = time.time()
        cutoff_time = current_time - self.cleanup_interval
        print(f"Running cleanup task at {current_time}, removing peers with last_seen before {cutoff_time}")
        with self.conn:
            cursor = self.conn.execute('''
                DELETE FROM peers WHERE last_seen < ?
            ''', (cutoff_time,))
            print(f"Deleted {cursor.rowcount} stale peers")

if __name__ == '__main__':
    root = resource.Resource()
    root.putChild(b"tracker", Tracker()) # filho que gerencia requisições relacionadas aos peers dos torrents
    root.putChild(b"dados", TorrentTracker()) # filho que gerencia requisições relacionadas aos dados dos torrents no banco de dados

    site = server.Site(root) #  responsavel por gerenciar todas as requisições HTTP recebidas
    reactor.listenTCP(6969, site) #configurando o reactor para escutar na porta 6969
    print("Server is running on port 6969")
    reactor.run()
