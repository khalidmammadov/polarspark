from pathlib import Path
from typing import Generator

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from polarspark.sql.utils import NO_INPUT


def watch_files(path: str, recursive: bool = False) -> Generator[Path, None, None]:
    path = Path(path)

    # 1. Yield pre-existing files
    for file in path.rglob("*") if recursive else path.iterdir():
        if file.is_file():
            yield file

    # 2. Setup watchdog for new ones
    class _Handler(FileSystemEventHandler):
        def __init__(self):
            self.queue = []

        def on_created(self, event):
            if not event.is_directory:
                self.queue.append(Path(event.src_path))

    handler = _Handler()
    observer = Observer()
    observer.schedule(handler, str(path), recursive=recursive)
    observer.start()

    try:
        while True:
            # Yield files as they appear
            while handler.queue:
                yield handler.queue.pop(0)
            yield NO_INPUT
    finally:
        observer.stop()
        observer.join()
