import asyncio
import logging
import random
import time
import traceback
from decimal import ROUND_DOWN, Decimal
from typing import Awaitable, List

from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import PositionMode
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

PRECISION: int = 10 ** 18
TIME_PRECISION: int = 10 ** 9

global task_logger
task_logger = logging.getLogger(__name__)


def now() -> int:
    return int(time.time())


def now_ns() -> int:
    return to_nano_second(time.time())


def to_nano_second(timestamp: float) -> int:
    return int(timestamp * TIME_PRECISION)


def to_fixed_point(val: float | Decimal, precision: int = PRECISION) -> int:
    return int(Decimal(val) * Decimal(precision))


def to_decimal(val: int, precision: int = PRECISION) -> Decimal:
    return Decimal(val) / Decimal(precision)


def quantize(val: Decimal, quantum: Decimal, rounding=ROUND_DOWN) -> Decimal:
    return (val.quantize(quantum, rounding) // quantum) * quantum


def get_positions(market_pair: MarketTradingPairTuple) -> List[Position]:
    trading_pair = market_pair.trading_pair
    positions: List[Position] = [
        position
        for position in market_pair.market.account_positions.values()
        if not isinstance(position, PositionMode) and position.trading_pair == trading_pair
    ]
    return positions


def get_account_position(market_pair: MarketTradingPairTuple) -> Decimal:
    positions = get_positions(market_pair)
    amount = Decimal(0)
    for position in positions:
        amount += position.amount
    return amount


def to_symbol(pair: str) -> str:
    return pair.replace("/", "-")


def to_pair(symbol: str) -> str:
    return symbol.replace("-", "/")


async def safe_wrapper(awaitable: Awaitable):
    try:
        return await awaitable
    except Exception as e:
        task_logger.info(f"unhandled exception in task {e}")
        task_logger.info(traceback.format_exc())


def safe_create_task(awaitable: Awaitable) -> asyncio.Task:
    return asyncio.create_task(safe_wrapper(awaitable))


def apply_variability(value, variability_percent):
    min_factor = 1 - (variability_percent / 100)
    max_factor = 1 + (variability_percent / 100)
    variability_factor = random.uniform(min_factor, max_factor)
    return int(value * variability_factor)
