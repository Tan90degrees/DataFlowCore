"""HTTP embedding fixture for acceptance tests; not a semantic model service."""

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .ingestion import fixture_vector


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        with self.server.lock:
            self.server.active += 1
            self.server.peak_active = max(self.server.peak_active, self.server.active)
            self.server.calls += 1
            status = self.server.error_status if self.server.failures else 200
            self.server.failures = max(0, self.server.failures - 1)
        try:
            length = int(self.headers.get("Content-Length", 0))
            if not 0 < length <= 1_000_000:
                self.send_error(400)
                return
            data = json.loads(self.rfile.read(length))
            time.sleep(self.server.delay)
            result = json.dumps({"vector": fixture_vector(data["text"])}).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(result)))
            self.end_headers()
            try:
                self.wfile.write(result)
            except BrokenPipeError, ConnectionResetError:
                pass
        finally:
            with self.server.lock:
                self.server.active -= 1

    def log_message(self, *_):
        pass


class FixtureServer(ThreadingHTTPServer):
    request_queue_size = 256

    def __init__(self, address=("127.0.0.1", 0), delay=0):
        super().__init__(address, Handler)
        self.lock = threading.Lock()
        self.delay, self.failures, self.error_status = delay, 0, 503
        self.active, self.peak_active, self.calls = 0, 0, 0
        self.url = f"http://{address[0]}:{self.server_port}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--delay", type=float, default=0.01)
    args = parser.parse_args()
    server = FixtureServer((args.host, args.port), args.delay)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
