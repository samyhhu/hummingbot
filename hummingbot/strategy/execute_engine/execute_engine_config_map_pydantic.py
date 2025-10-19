from decimal import Decimal
from typing import Dict, Union

from pydantic import Field, validator

from hummingbot.client.config.config_data_types import BaseClientModel, ClientConfigEnum, ClientFieldData
from hummingbot.client.config.config_validators import validate_bool, validate_market_trading_pair
from hummingbot.client.config.strategy_config_data_types import BaseStrategyConfigMap
from hummingbot.client.settings import AllConnectorSettings

ExchangeEnum = ClientConfigEnum(  # rebuild the exchanges enum
    value="Exchanges",  # noqa: F821
    names={e: e for e in AllConnectorSettings.get_connector_settings()},
    type=str,
)


def get_field(i: int) -> Field:
    return Field(
        default="",
        description="The name of the exchange connector.",
        client_data=ClientFieldData(
            prompt=lambda mi: f"Do you want to enable connector {i}? (y/n)",
            prompt_on_new=True,
        ),
    )


MAX_CONNECTOR = 5


class EmptyMarketConfigMap(BaseClientModel):
    connector: Union[None, ExchangeEnum] = None
    market: Union[None, str] = None

    class Config:
        title = "n"


class MarketConfigMap(BaseClientModel):
    connector: Union[None, ExchangeEnum] = Field(
        default=...,
        description="The name of the exchange connector.",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter name of the exchange to use",
            prompt_on_new=True,
        ),
    )
    market: Union[None, str] = Field(
        default=...,
        description="The name of the trading pair.",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter name of trading-pair on this venue",
            prompt_on_new=True,
        ),
    )

    @validator("market", pre=True)
    def validate_market(cls, market: Union[str, str], values: Dict):
        if market is None:
            return None
        validated = validate_market_trading_pair(values["connector"], market)
        if validated:
            return validated
        return market

    class Config:
        title = "y"


market_config_map = Union[EmptyMarketConfigMap, MarketConfigMap]


class ExecuteEngineConfigMap(BaseStrategyConfigMap):
    strategy: str = Field(default="execute_engine", client_data=None)
    execute_interval: int = Field(
        default=1,
        description="The interval in seconds to check for execution.",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the interval in seconds to check for execution",
            prompt_on_new=True,
        ),
    )
    slippage: Decimal = Field(
        default=Decimal("0.001"),
        description="The slippage tolerance for the execute order.",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the slippage tolerance for the execute order",
            prompt_on_new=True,
        ),
    )
    execute_leverage: int = Field(
        default=2,
        description="The leverage to use for the market.",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the leverage to use for the execute market",
            prompt_on_new=True,
        ),
    )
    execute_connector_0: market_config_map = get_field(0)
    execute_connector_1: market_config_map = get_field(1)
    execute_connector_2: market_config_map = get_field(2)
    execute_connector_3: market_config_map = get_field(3)
    execute_connector_4: market_config_map = get_field(4)

    @validator("execute_connector_0", "execute_connector_1", "execute_connector_2", "execute_connector_3", "execute_connector_4", pre=True)
    def construct_connector(cls, v: Union[str, bool, EmptyMarketConfigMap, MarketConfigMap, Dict]):
        if isinstance(v, (EmptyMarketConfigMap, MarketConfigMap, Dict)):
            return v
        if validate_bool(v):
            raise ValueError("enter a boolean value")
        if v.lower() in (True, "true", "yes", "y"):
            return MarketConfigMap.construct()
        return EmptyMarketConfigMap.construct()
