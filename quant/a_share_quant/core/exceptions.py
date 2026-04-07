"""核心异常定义。"""


class QuantError(RuntimeError):
    """工程通用基础异常。"""


class DataValidationError(QuantError):
    """数据不满足约束时抛出。"""


class DataSourceError(QuantError):
    """外部数据源调用或映射失败时抛出。"""


class ExternalDependencyError(QuantError):
    """可选三方依赖未安装时抛出。"""


class OrderRejectedError(QuantError):
    """订单被执行层拒绝时抛出。"""


class RiskRejectedError(QuantError):
    """订单被风控拒绝时抛出。"""


class ExternalServiceTimeoutError(QuantError):
    """外部服务调用超过配置时限时抛出。"""


class BrokerContractError(QuantError):
    """券商客户端返回载荷不满足领域契约时抛出。"""
