import logging
from pathlib import Path
from setting import SETTINGS


class Logger:
    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        self.formatter = logging.Formatter(SETTINGS["log.format"])

        if SETTINGS["log.console"]:
            self.console_handler = logging.StreamHandler()
            self.console_handler.setLevel(logging.INFO)
            self.console_handler.setFormatter(self.formatter)
            self.logger.addHandler(self.console_handler)

        self.file_handler = None

    def add_file_handler(self, file_name: str) -> None:
        file_dir: Path = Path(SETTINGS["project.abs_path"]).joinpath(SETTINGS["log.file_direction"])
        file_path: Path = file_dir.joinpath(file_name)
        if not str(file_path).endswith(".log"):
            file_path = Path(str(file_path) + ".log")
        self.file_handler = logging.FileHandler(file_path)
        self.file_handler.setLevel(logging.WARNING)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def critical(self, message: str) -> None:
        self.logger.critical(message)