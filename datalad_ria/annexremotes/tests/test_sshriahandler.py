

from ..ssh_riahandler import DownloadProgressRG


def test_download_basics():
    class Dummy:
        def __init__(self):
            self.stderr_deque = None

    def progress_callback(size, total_size):
        print(size, total_size)

    x = DownloadProgressRG(Dummy(), progress_callback)
    print(x)
