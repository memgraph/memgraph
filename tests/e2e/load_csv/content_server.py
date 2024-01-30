from http.server import BaseHTTPRequestHandler, HTTPServer


class ContentServer:
    def __init__(self, content, compressor=None):
        content = content.encode("utf-8")
        if compressor is not None:
            content = compressor(content)
        self.content = content

    def http_server(self):
        content = self.content

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/plain")
                self.end_headers()
                self.wfile.write(content)

        return HTTPServer(("", 0), Handler)
