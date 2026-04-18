"""Agent基类框架 - 定义标准接口和执行模板"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from .config import Config
from .exceptions import AgentError, AgentValidationError
from .llm import LLMRouter, llm_router
from .logging import get_logger
from .models import AgentExecutionRecord, AgentInput, AgentOutput, AgentStatus

InputType = TypeVar("InputType", bound=AgentInput)
OutputType = TypeVar("OutputType", bound=AgentOutput)


class AgentBase(ABC, Generic[InputType, OutputType]):
    """所有Agent的基类

    子类必须:
    1. 设置 agent_name 类属性
    2. 实现 run() 方法
    3. 实现 validate_output() 方法

    外部调用 safe_run()，不要直接调用 run()。
    """

    agent_name: str = "base"
    execution_mode: str = "deterministic"

    def __init__(self) -> None:
        self.config = Config()
        self.llm: LLMRouter = llm_router
        self.logger = get_logger(f"agent.{self.agent_name}")

    @abstractmethod
    async def run(self, input_data: InputType) -> OutputType:
        """执行Agent主逻辑（子类实现）"""
        ...

    @abstractmethod
    def validate_output(self, output: OutputType) -> None:
        """校验输出，失败抛出 AgentValidationError"""
        ...

    async def safe_run(self, input_data: InputType) -> OutputType:
        """带异常捕获和日志的执行入口

        这是外部调用的入口，不要直接调用 run()。
        """
        self.logger.info(f"Agent[{self.agent_name}] 开始执行 | input={input_data.stock_code}")
        try:
            result = await self.run(input_data)
            self._apply_runtime_metadata(result)
            self.validate_output(result)
            self.logger.info(f"Agent[{self.agent_name}] 执行完成")
            return result
        except AgentValidationError:
            self.logger.error(f"Agent[{self.agent_name}] 输出校验失败")
            raise
        except Exception as e:
            self.logger.error(f"Agent[{self.agent_name}] 执行异常: {e}")
            raise AgentError(self.agent_name, str(e)) from e

    def _configured_model(self) -> str | None:
        getter = getattr(self, "_get_model", None)
        if not callable(getter):
            return None
        try:
            model = getter()
        except Exception:
            return None
        if isinstance(model, str) and model:
            return model
        return None

    def _apply_runtime_metadata(self, output: OutputType) -> None:
        if not output.execution_mode:
            output.execution_mode = self.execution_mode
        if output.configured_model is None:
            output.configured_model = self._configured_model()
        if output.llm_invoked and output.model_used is None:
            output.model_used = output.configured_model

    def build_execution_record(self, output: AgentOutput) -> AgentExecutionRecord:
        self._apply_runtime_metadata(output)
        return AgentExecutionRecord(
            agent_name=output.agent_name,
            status=output.status.value,
            execution_mode=output.execution_mode or self.execution_mode,
            configured_model=output.configured_model,
            model_used=output.model_used,
            llm_invoked=output.llm_invoked,
            summary=output.summary,
            confidence=output.confidence,
            errors=list(output.errors),
        )
