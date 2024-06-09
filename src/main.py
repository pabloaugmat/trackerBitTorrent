import time
import re
import bencodepy
from twisted.internet import reactor
from twisted.web import server, resource
from twisted.internet.task import LoopingCall

class TorrentTracker(resource.Resource):
    isLeaf = True

    def __init__(self):
        self.peers = {}
        self.cleanup_interval = 1800  # Intervalo para limpeza em segundos

        # Configura uma chamada periódica para a limpeza de peers
        self.cleanup_task = LoopingCall(self.cleanup_all_peers)
        self.cleanup_task.start(self.cleanup_interval)

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

        # Adiciona ou atualiza o peer
        current_time = time.time()
        if info_hash not in self.peers:
            self.peers[info_hash] = {}
        
        self.peers[info_hash][peer_id] = {
            'ip': ip,
            'port': port,
            'last_seen': current_time
        }

        # Limpa peers inativos
        self.cleanup_peers(info_hash, current_time)

        # Prepara a resposta com a lista de peers
        response = {
            'interval': self.cleanup_interval,
            'peers': [{'ip': peer['ip'], 'port': peer['port']} for peer in self.peers[info_hash].values()]
        }
        
        return bencodepy.encode(response)

    def cleanup_all_peers(self):
        current_time = time.time()
        for info_hash in list(self.peers.keys()):
            self.cleanup_peers(info_hash, current_time)

    def cleanup_peers(self, info_hash, current_time):
        inativos = [peer_id for peer_id, peer in self.peers[info_hash].items() if current_time - peer['last_seen'] > self.cleanup_interval]
        for peer_id in inativos:
            del self.peers[info_hash][peer_id]

if __name__ == '__main__':
    tracker = TorrentTracker()
    site = server.Site(tracker)
    reactor.listenTCP(6969, site)
    reactor.run()
