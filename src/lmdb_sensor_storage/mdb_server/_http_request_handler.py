import logging
import os
import shutil
import zlib
from http import HTTPStatus
import http.server
import http

from lmdb_sensor_storage.mdb_server import favicon

logger = logging.getLogger('lmdb_sensor_storage.httpd')


# noinspection PyPep8Naming
class HTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """
    HTTP request handler with support for chuncking and compression in do_GET.
    Chunked encoding allows the server to send data in chunks. This is used to
    stream data or send data which would not fit in the buffer as a whole.

    References
    ----------
    https://en.wikipedia.org/wiki/Chunked_transfer_encoding
    """

    def do_GET(self):

        if self.path == '/favicon.ico':
            self.send_response(200)
            self.send_header('Content-Type', 'image/vnd.microsoft.icon')
            self.send_header('Content-Length', str(len(favicon)))
            self.end_headers()

            self.wfile.write(favicon)

        elif self.path == '/plotly.min.js':
            # serve plotly.min.js from plotly package since some sensors are in a confined environment with no internet
            # access
            try:
                import plotly
            except ImportError:
                self.send_error(501, message='Plotly is not installed properly')
                return
            self.path = os.path.join(os.path.dirname(plotly.__file__),
                                     'package_data', 'plotly.min.js')

            self._serve_file_absolute_path()

        else:  # any other path
            self.send_response(404)
            self.end_headers()

    def _serve_file_absolute_path(self, extra_headers=None):
        """
        Serve file `self.path`, when `self.path` is an absolute path.
        Absolute paths do not work with super().GET(), since `self.translate` used in super().do_GET() always
        returns a relativ path.

        See: https://stackoverflow.com/questions/67359204/how-to-run-python-with-simplehttprequesthandler
        """

        # use gzip compression if browser is not on localhost (gzip will take longer than transfer)
        use_gzip = 'gzip' in self.headers.get('Accept-Encoding', '').split(', ') and not self.is_localhost()

        try:
            with open(self.path, 'rb') as f:
                fs = os.fstat(f.fileno())
                self.send_response(200)
                # self.send_header("Content-Type", self.guess_type(self.path))
                self.send_header("Last-Modified", self.date_time_string(int(fs.st_mtime)))
                self.send_header("Cache-Control", "public, max-age=31536000")
                if use_gzip:
                    self.send_chunked_header()
                else:
                    self.send_header("Content-Length", str(fs[6]))
                if extra_headers is not None:
                    for key, val in extra_headers.items():
                        self.send_header(key, val)
                self.end_headers()

                if use_gzip:
                    # noinspection PyUnboundLocalVariable
                    self.write_chunked(f.read())
                    self.end_write_chunked()
                else:
                    shutil.copyfileobj(f, self.wfile)

        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return

    def send_chunked_header(self):
        """
        Uses `self.send_header` to set 'Transfer-Encoding' and 'Content-Encoding'
        """
        self.send_header('Transfer-Encoding', 'chunked')
        if 'gzip' in self.headers.get('Accept-Encoding', '').split(', '):
            self.send_header('Content-Encoding', 'gzip')

    def write_chunked(self, data: bytes):
        """
        Like `self.write`, but transparently uses chunked encoding and compression.
        To enable usage of `self.write_chunked`, `self.send_chunked_headers` must be used to set
        HTTP headers to inform client about chunked encoding.
        Can be called multiple times after `self.end_headers()`.
        After last use of `write_chunked`, `self.end_write_chunked` must be used to flush buffers.

        """
        if 'gzip' in self.headers.get('Accept-Encoding', '').split(', '):
            if not hasattr(self, '_cmp'):
                # noinspection PyAttributeOutsideInit
                self._cmp = zlib.compressobj(-1, zlib.DEFLATED, zlib.MAX_WBITS | 16)
            data = self._cmp.compress(data)

        self._write_chunk(data)

    def _write_chunk(self, data: bytes):
        if data:
            self.wfile.write('{:X}\r\n'.format(len(data)).encode())
            self.wfile.write(data)
            self.wfile.write('\r\n'.encode())

    def end_write_chunked(self):
        """
        Must be used after last call to `self.write_chunked` to send end-of-message string.
        """
        if hasattr(self, '_cmp'):
            ret = self._cmp.flush()

            # force delete _cmp so for the next request a new instance is generated
            del self._cmp

            self._write_chunk(ret)

        self.wfile.write('0\r\n\r\n'.encode())

    # noinspection PyShadowingBuiltins,PyShadowingNames
    def log_message(self, format, *args):
        """
        Override logging function, since per default every request is printed to stderr
        """

        if logger.level == logging.debug:
            logger.debug("%s %s",
                         self.address_string(), format % args)
        elif not self.is_localhost():
            logger.info("%s %s",
                        self.address_string(), format % args)

    def is_localhost(self):
        # noinspection SpellCheckingInspection
        return self.client_address[0] in ('127.0.0.1',
                                          '127.0.0.2',
                                          '::ffff:127.0.0.1',
                                          '::1')
