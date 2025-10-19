import logging
import math
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, root_validator

import hummingbot.strategy.execute_engine.utils as utils
from hummingbot.core.data_type.trade_fee import TokenAmount

global algo_logger
algo_logger = logging.getLogger(__name__)


class AlgoType(Enum):
    UNKNOWN = 0
    BASIC_TWAP = 1
    HEDGE_TWAP = 2


class AlgoRequest(BaseModel):
    time: int
    order_id: int
    market_pair: str
    side: str
    amount: str
    slippage_threshold: str
    duration_slice: str
    duration: str
    algo_type: str = "UNKNOWN"
    hedge_market_pair: str = ""
    basis_threshold: str = "nan"
    percent_floating: str = "nan"


class AlgoChildOrder(BaseModel):
    time: int
    market_pair_name: str

    algo_order_id: int
    order_id: str
    is_hedge: bool

    is_buy: bool
    price: Decimal
    amount: Decimal

    amount_filled: Decimal = Decimal(0)
    last_cancel_time: int = 0

    def is_fully_filled(self) -> bool:
        return math.isclose(self.amount_filled, self.amount) or self.amount_filled >= self.amount

    def unfilled_amount(self) -> Decimal:
        return self.amount - self.amount_filled


class AlgoOrder(BaseModel):
    request: AlgoRequest
    start_time: int

    end_time: int = 0
    duration: int = 0
    algo_type: AlgoType = AlgoType.UNKNOWN

    rejected_count: int = 0

    canceled: bool = False
    failed: bool = False
    updated: bool = False

    prev_execute_time: int = 0
    next_execute_time: int = 0

    is_buy: bool = False
    amount: Decimal = Decimal(0)
    slippage_threshold: Decimal = Decimal(0)
    basis_threshold: Decimal = Decimal("nan")
    percent_floating: Decimal = Decimal("nan")

    interval: int = 0
    interval_amount: Decimal = Decimal(0)

    # ---------------
    amount_filled_pending: Decimal = Decimal(0)
    amount_filled: Decimal = Decimal(0)

    amount_hedged_pending: Decimal = Decimal(0)
    amount_hedged: Decimal = Decimal(0)

    amount_price_filled: Decimal = Decimal(0)
    amount_price_hedged: Decimal = Decimal(0)

    pending_order: Optional[AlgoChildOrder] = None
    pending_hedge_order: Optional[AlgoChildOrder] = None

    fees: Dict[str, Decimal] = {}

    @root_validator(pre=False, skip_on_failure=True)
    def _init_algo(cls, values: Dict) -> Dict:
        values["duration"] = int(values["request"].duration) * utils.TIME_PRECISION
        values["end_time"] = values["start_time"] + values["duration"]
        values["algo_type"] = AlgoType[values["request"].algo_type]
        values["prev_execute_time"] = values["start_time"]
        values["next_execute_time"] = values["start_time"]

        values["is_buy"] = values["request"].side == "buy"
        values["amount"] = Decimal(values["request"].amount)
        values["basis_threshold"] = Decimal(values["request"].basis_threshold)
        values["percent_floating"] = Decimal(values["request"].percent_floating)
        values["slippage_threshold"] = Decimal(values["request"].slippage_threshold)

        values["interval"] = int(values["request"].duration) // int(values["request"].duration_slice) * utils.TIME_PRECISION
        values["interval_amount"] = Decimal(values["request"].amount) / Decimal(values["request"].duration_slice)
        return values

    @property
    def order_id(self):
        return self.request.order_id

    def is_updated(self):
        return self.updated

    def is_canceled(self):
        return self.canceled

    def is_failed(self):
        return self.failed

    def is_expired(self, current_time) -> bool:
        return current_time > self.end_time + 10 * utils.TIME_PRECISION

    def is_fully_filled(self) -> bool:
        return self.amount_filled >= self.amount

    def is_hedged(self) -> bool:
        return self.amount_hedged >= self.amount

    def is_order_pending(self) -> bool:
        return self.pending_order is not None

    def is_hedge_order_pending(self) -> bool:
        return self.pending_hedge_order is not None

    def avg_fill_price(self) -> Decimal:
        if self.amount_filled.is_zero():
            return Decimal(0)
        return self.amount_price_filled / self.amount_filled

    def avg_hedge_price(self) -> Decimal:
        if self.amount_hedged.is_zero():
            return Decimal(0)
        return self.amount_price_hedged / self.amount_hedged

    def handle_order_created(self, child_order: AlgoChildOrder):
        if not child_order.is_hedge:
            algo_logger.info(f"algo {child_order.algo_order_id} tracking child order {child_order.order_id}")
            self.amount_filled_pending += child_order.amount
            self.pending_order = child_order
        else:
            algo_logger.info(f"algo {child_order.algo_order_id} tracking hedge child order {child_order.order_id}")
            self.amount_hedged_pending += child_order.amount
            self.pending_hedge_order = child_order

    def handle_order_failed(self, child_order_id: str):
        self.rejected_count += 1
        self.handle_order_canceled(child_order_id)

    def handle_order_canceled(self, child_order_id: str):
        if self.pending_order is not None and child_order_id == self.pending_order.order_id:
            self.amount_filled_pending -= self.pending_order.unfilled_amount()
            algo_logger.info(f"child order {self.pending_order.order_id} canceled")
            self.pending_order = None
        elif self.pending_hedge_order is not None and child_order_id == self.pending_hedge_order.order_id:
            self.amount_hedged_pending -= self.pending_hedge_order.unfilled_amount()
            algo_logger.info(f"child order {self.pending_hedge_order.order_id} canceled")
            self.pending_hedge_order = None

    def handle_order_filled(self, child_order_id: str, price: Decimal, amount: Decimal, current_time):
        if self.pending_order is not None and child_order_id == self.pending_order.order_id:
            self.amount_filled += amount
            self.amount_price_filled += price * amount
            self.amount_filled_pending -= amount
            self.pending_order.amount_filled += amount
            if self.pending_order.is_fully_filled():
                algo_logger.info(f"child order {self.pending_order.order_id} fully filled")
                self.pending_order = None
            else:
                algo_logger.info(f"child order {self.pending_order.order_id} partially filled, unfilled {self.pending_order.unfilled_amount()}")
                if self.is_expired(current_time):
                    algo_logger.info(f"child spot order {self.pending_order.order_id} expired")
        elif self.pending_hedge_order is not None and child_order_id == self.pending_hedge_order.order_id:
            self.amount_hedged += amount
            self.amount_price_hedged += price * amount
            self.amount_hedged_pending -= amount
            self.pending_hedge_order.amount_filled += amount
            if self.pending_hedge_order.is_fully_filled():
                algo_logger.info(f"child order {self.pending_hedge_order.order_id} fully filled")
                self.pending_hedge_order = None
            else:
                algo_logger.info(f"child hedge order {self.pending_hedge_order.order_id} partially filled, unfilled {self.pending_hedge_order.unfilled_amount()}")
                if self.is_expired(current_time):
                    algo_logger.info(f"child hedge order {self.pending_hedge_order.order_id} expired")
        else:
            algo_logger.error(f"child order {child_order_id} not matching any of the pending orders")

    def handle_order_fees(self, flat_fees: List[TokenAmount]):
        for fee in flat_fees:
            if fee.token not in self.fees:
                self.fees[fee.token] = Decimal(0)
            self.fees[fee.token] += fee.amount
