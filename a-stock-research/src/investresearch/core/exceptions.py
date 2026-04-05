"""自定义异常层次结构"""


class InvestResearchError(Exception):
    """系统基础异常"""
    pass


class ConfigurationError(InvestResearchError):
    """配置异常"""

    def __init__(self, message: str = "", *, config_path: str | None = None):
        self.config_path = config_path
        detail = f" (配置路径: {config_path})" if config_path else ""
        super().__init__(f"{message}{detail}")


class LLMError(InvestResearchError):
    """LLM调用异常"""
    pass


class LLMRateLimitError(LLMError):
    """LLM限流异常"""

    def __init__(self, provider: str, retry_after: int | None = None):
        self.provider = provider
        self.retry_after = retry_after
        msg = f"提供商 {provider} 触发限流"
        if retry_after:
            msg += f"，建议 {retry_after}s 后重试"
        super().__init__(msg)


class LLMResponseError(LLMError):
    """LLM响应解析异常"""

    def __init__(self, message: str, *, raw_response: str | None = None):
        self.raw_response = raw_response
        super().__init__(message)


class DataCollectionError(InvestResearchError):
    """数据采集异常"""

    def __init__(self, source: str, message: str = "", *, stock_code: str | None = None):
        self.source = source
        self.stock_code = stock_code
        detail = f"[{source}]"
        if stock_code:
            detail += f" 标的={stock_code}"
        super().__init__(f"{detail} {message}")


class AgentError(InvestResearchError):
    """Agent执行异常"""

    def __init__(self, agent_name: str, message: str):
        self.agent_name = agent_name
        super().__init__(f"[{agent_name}] {message}")


class AgentValidationError(AgentError):
    """Agent输出校验异常"""

    def __init__(self, agent_name: str, errors: list[str]):
        self.validation_errors = errors
        super().__init__(agent_name, f"输出校验失败: {'; '.join(errors)}")


class AgentTimeoutError(AgentError):
    """Agent超时异常"""

    def __init__(self, agent_name: str, timeout_seconds: float):
        self.timeout_seconds = timeout_seconds
        super().__init__(agent_name, f"执行超时 ({timeout_seconds:.0f}s)")


class ResearchPipelineError(InvestResearchError):
    """研究流程异常"""

    def __init__(self, step: str, message: str):
        self.step = step
        super().__init__(f"[流程:{step}] {message}")


# ============================================================
# Phase 7: 知识库异常
# ============================================================


class KnowledgeBaseError(InvestResearchError):
    """知识库异常"""

    def __init__(self, message: str, *, collection: str | None = None):
        self.collection = collection
        detail = f" (集合: {collection})" if collection else ""
        super().__init__(f"{message}{detail}")


class KnowledgeBaseConnectionError(KnowledgeBaseError):
    """知识库连接异常"""


class KnowledgeBaseQueryError(KnowledgeBaseError):
    """知识库查询异常"""
