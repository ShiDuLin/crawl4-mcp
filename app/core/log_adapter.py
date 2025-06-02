import logging
import sys
from types import TracebackType

import structlog
from structlog.stdlib import get_logger
from structlog.types import EventDict, Processor

# 创建日志记录器
logger = get_logger()


def drop_color_message_key(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    """
    去除Uvicorn日志中多余的color_message键
    """
    event_dict.pop("color_message", None)
    return event_dict


def setup_logging(json_logs: bool = False, log_level: str = "INFO") -> None:
    """
    设置日志配置

    Args:
        json_logs (bool): 是否使用JSON格式输出日志
        log_level (str): 日志级别，例如"INFO"、"DEBUG"、"ERROR"等
    """
    # 配置时间戳格式
    timestamper = structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S%Z.%f", utc=False)

    # 基础日志处理器
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        drop_color_message_key,
        timestamper,
        structlog.processors.StackInfoRenderer(),
    ]

    # JSON日志特定处理
    if json_logs:
        # 将event键重命名为message（适用于Datadog等工具）
        shared_processors.append(structlog.processors.EventRenamer("message"))
        # 格式化异常信息
        shared_processors.append(structlog.processors.format_exc_info)

    # 配置structlog
    structlog.configure(
        processors=shared_processors
        + [
            # 为ProcessorFormatter准备事件字典
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # 根据配置选择日志渲染器
    log_renderer: structlog.types.Processor
    if json_logs:
        log_renderer = structlog.processors.JSONRenderer()
    else:
        # 使用控制台渲染器，带有美观的异常格式
        log_renderer = structlog.dev.ConsoleRenderer(
            exception_formatter=structlog.dev.RichTracebackFormatter(show_locals=False)
        )

    # 创建处理器格式化程序
    formatter = structlog.stdlib.ProcessorFormatter(
        # 这些处理器仅用于非structlog来源的日志条目
        foreign_pre_chain=shared_processors,
        # 这些处理器用于所有日志条目
        processors=[
            # 移除_record和_from_structlog元数据
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            log_renderer,
        ],
    )

    # 配置控制台输出处理器
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level.upper())

    # 配置FastMCP相关日志记录器（如果存在）
    for logger_name in ["fastmcp", "fastmcp.server", "fastmcp.tools"]:
        _logger = logging.getLogger(logger_name)
        _logger.handlers.clear()
        _logger.addHandler(handler)
        _logger.propagate = False
        _logger.setLevel(log_level.upper())

    # 配置uvicorn日志记录器
    logging.getLogger("uvicorn").handlers.clear()

    # 配置uvicorn错误日志
    uvicorn_error = logging.getLogger("uvicorn.error")
    uvicorn_error.handlers.clear()
    uvicorn_error.addHandler(handler)
    uvicorn_error.propagate = False
    uvicorn_error.setLevel(logging.ERROR)

    # 配置uvicorn访问日志
    uvicorn_access = logging.getLogger("uvicorn.access")
    uvicorn_access.handlers.clear()
    uvicorn_access.addHandler(handler)
    uvicorn_access.setLevel(logging.INFO)
    uvicorn_access.propagate = False

    # 配置全局异常处理
    def handle_exception(
        exc_type: type[BaseException], exc_value: BaseException, exc_traceback: TracebackType | None
    ) -> None:
        """全局未捕获异常处理器"""
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        root_logger.error("未捕获异常", exc_info=(exc_type, exc_value, exc_traceback))

    # 注册全局异常处理器
    sys.excepthook = handle_exception
