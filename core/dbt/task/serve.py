import shutil
import os
import webbrowser

from dbt.include.global_project import DOCS_INDEX_FILE_PATH
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
from dbt.events.functions import fire_event
from dbt.events.types import ServingDocsPortport, ServingDocsAccessInfo, ServingDocsExitInfo

from dbt.task.base import ConfiguredTask


class ServeTask(ConfiguredTask):
    def run(self):
        os.chdir(self.config.target_path)

        port = self.args.port

        shutil.copyfile(DOCS_INDEX_FILE_PATH, 'index.html')

        fire_event(ServingDocsPortport(port))
        fire_event(ServingDocsAccessInfo(port))
        fire_event(ServingDocsExitInfo())

        # mypy doesn't think SimpleHTTPRequestHandler is ok here, but it is
        httpd = TCPServer(  # type: ignore
            ('0.0.0.0', port),
            SimpleHTTPRequestHandler  # type: ignore
        )  # type: ignore

        if self.args.open_browser:
            try:
                webbrowser.open_new_tab(f'http://127.0.0.1:{port}')
            except webbrowser.Error:
                pass

        try:
            httpd.serve_forever()  # blocks
        finally:
            httpd.shutdown()
            httpd.server_close()

        return None
