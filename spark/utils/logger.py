import logging
import sys
from datetime import datetime
from typing import Optional


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class ETLLogger:
    def __init__(self, job_name: str):
        self.logger = get_logger(job_name)
        self.job_name = job_name
        self._start_times: dict = {}

    def start_phase(self, phase: str) -> None:
        self._start_times[phase] = datetime.now()
        self.logger.info(f"[INICIO] Fase: {phase}")

    def end_phase(self, phase: str, records: Optional[int] = None) -> None:
        if phase in self._start_times:
            duration = (datetime.now() - self._start_times[phase]).total_seconds()
            msg = f"[FIM] Fase: {phase} | Duracao: {duration:.2f}s"
            if records is not None:
                msg += f" | Registros: {records:,}"
            self.logger.info(msg)
        else:
            self.logger.warning(f"[FIM] Fase: {phase} | Tempo de inicio nao registrado")

    def log_extraction(self, table: str, count: int) -> None:
        self.logger.info(f"[EXTRACT] Tabela: {table} | Registros lidos: {count:,}")

    def log_transform(self, step: str, count: int) -> None:
        self.logger.info(f"[TRANSFORM] Etapa: {step} | Registros: {count:,}")

    def log_load(self, path: str, count: int, format: str = "CSV") -> None:
        self.logger.info(f"[LOAD] Formato: {format} | Registros: {count:,} | Destino: {path}")

    def log_error(self, phase: str, error: Exception) -> None:
        self.logger.error(f"[ERRO] Fase: {phase} | Tipo: {type(error).__name__} | Msg: {str(error)}")

    def info(self, msg: str) -> None:
        self.logger.info(msg)

    def warning(self, msg: str) -> None:
        self.logger.warning(msg)

    def error(self, msg: str) -> None:
        self.logger.error(msg)