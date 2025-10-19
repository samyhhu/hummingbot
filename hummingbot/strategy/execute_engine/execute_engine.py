import asyncio
import logging
import traceback
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd

import hummingbot.strategy.execute_engine.api_server as api_server
import hummingbot.strategy.execute_engine.constants as constants
import hummingbot.strategy.execute_engine.utils as utils
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.execute_engine.data_types import AlgoChildOrder, AlgoOrder, AlgoRequest, AlgoType
from hummingbot.strategy.execute_engine.db_manager import DbManager
from hummingbot.strategy.execute_engine.execute_engine_config_map_pydantic import ExecuteEngineConfigMap
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase


class ExecuteEngineStrategy(StrategyPyBase):
    """
    Execution Engine for
    """

    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        config_map: ExecuteEngineConfigMap,
        market_pairs: List[MarketTradingPairTuple],
        client_config_map: ClientConfigMap,
        database_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._config_map = config_map
        self._market_pairs = market_pairs

        # Strategy Setup Variables
        all_hedge_markets = list(set([market_pair.market for market_pair in self._market_pairs]))
        self.add_markets(all_hedge_markets)

        # Execution Engine Objects
        self._hedge_markets_ready = False
        self._market_pairs_map: Dict[str, MarketTradingPairTuple] = {}
        self._algo_orders: Dict[int, AlgoOrder] = {}
        self._child_orders: Dict[str, AlgoChildOrder] = {}
        self._algo_request_queue = asyncio.Queue(maxsize=1)

        self._api_server = api_server.create_server(self)
        self._db_manager = DbManager(client_config_map, database_name)
        self._tasks: List[Tuple[str, asyncio.Task]] = []

    # Strategy Override Methods
    def start(self, clock: Clock, timestamp: float) -> None:
        self.logger().info("starting strategy")
        self.initialize_connectors()

        self._algo_orders: Dict[int, AlgoOrder] = {}
        self._child_orders: Dict[str, AlgoChildOrder] = {}

        self._tasks.append(("api_server", utils.safe_create_task(self._api_server.serve())))
        self._tasks.append(("poll_algo_request", utils.safe_create_task(self.poll_algo_request())))

    def stop(self, clock: Clock) -> None:

        self.logger().error(f"{traceback.print_exc()}")
        self.logger().info("stopping strategy")
        self.logger().info("shutting down api server")
        utils.safe_create_task(self._api_server.shutdown())

        for name, task in self._tasks:
            if task.cancel():
                self.logger().info(f"cancelled {name} task")
            else:
                self.logger().info(f"failed to cancel {name} task")
        self._tasks.clear()

    def initialize_connectors(self) -> None:
        leverage = self._config_map.execute_leverage
        position_mode = PositionMode.ONEWAY
        for market_pair in self._market_pairs:
            self._market_pairs_map[market_pair.name] = market_pair
            if "perpetual" not in market_pair.market.name:
                continue
            market = market_pair.market
            self.logger().info(f"setting {market.name} to mode {position_mode} with leverage {leverage}")
            market.set_leverage(market_pair.trading_pair, leverage)
            market.set_position_mode(position_mode)

    def tick(self, timestamp: float) -> None:
        markets_ready = True
        for market in self.active_markets:
            market_connected = market.network_status is NetworkStatus.CONNECTED
            if not market.ready or not market_connected:
                self.logger().warn(
                    f"market {market.name} is not ready yet network "
                    f"{market.network_status} status {market.status_dict}"
                )
                markets_ready = False
        self._hedge_markets_ready = markets_ready
        if not markets_ready:
            return

    def did_create_buy_order(self, order_created_event: BuyOrderCreatedEvent) -> None:
        if order_created_event.order_id not in self._child_orders:
            self.logger().error(f"received created order event for untracked child orders {order_created_event.order_id} ")
            return
        child_order = self._child_orders[order_created_event.order_id]
        if order_created_event.amount < child_order.amount:
            self.logger().info(f"received created order event with smaller amount {order_created_event.amount}, updating child order")
            child_order.amount = order_created_event.amount

    def did_create_sell_order(self, order_created_event: SellOrderCreatedEvent) -> None:
        if order_created_event.order_id not in self._child_orders:
            self.logger().error(f"received created order event for untracked child orders {order_created_event.order_id} ")
            return
        child_order = self._child_orders[order_created_event.order_id]
        if order_created_event.amount < child_order.amount:
            self.logger().info(f"received created order event with smaller amount {order_created_event.amount}, updating child order")
            child_order.amount = order_created_event.amount

    def did_complete_buy_order(self, order_completed_event: BuyOrderCompletedEvent) -> None:
        self.logger().info(f"buy order completed {order_completed_event}")
        self.handle_fill_order(order_completed_event.order_id)

    def did_complete_sell_order(self, order_completed_event: SellOrderCompletedEvent) -> None:
        self.logger().info(f"sell order completed {order_completed_event}")
        self.handle_fill_order(order_completed_event.order_id)

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent) -> None:
        self.logger().info(f"order failed {order_failed_event}")
        if order_failed_event.order_id not in self._child_orders:
            self.logger().error(f"received failed order event for untracked child orders {order_failed_event.order_id} ")
            return
        child_order = self._child_orders[order_failed_event.order_id]
        algo_order_id = child_order.algo_order_id
        if algo_order_id in self._algo_orders:
            algo_order = self._algo_orders[algo_order_id]
            algo_order.handle_order_failed(order_failed_event.order_id)
            if algo_order.rejected_count > constants.ALLOWED_FAILURES:
                algo_order.failed = True
        else:
            self.logger().error(
                f"order {order_failed_event.order_id} failed associated "
                f"algo order {algo_order_id} not in active algo order")

        self.logger().info(f"child order {child_order.order_id} canceled unfilled {child_order.unfilled_amount()}, remove tracking")
        del self._child_orders[child_order.order_id]

    def did_cancel_order(self, cancelled_event: OrderCancelledEvent) -> None:
        self.logger().info(f"order canceled {cancelled_event}")
        if cancelled_event.order_id not in self._child_orders:
            return
        child_order = self._child_orders[cancelled_event.order_id]

        algo_order_id = child_order.algo_order_id
        if algo_order_id in self._algo_orders:
            algo_order = self._algo_orders[algo_order_id]
            algo_order.handle_order_canceled(cancelled_event.order_id)
        else:
            self.logger().error(f"algo order {algo_order_id} not in active algo order")

        self.logger().info(f"child order {child_order.order_id} canceled unfilled {child_order.unfilled_amount()}, remove tracking")
        del self._child_orders[child_order.order_id]

    def did_fill_order(self, order_filled_event: OrderFilledEvent) -> None:
        self.logger().info(f"order filled event {order_filled_event}")
        self.handle_fill_order(order_filled_event.order_id, order_filled_event.price, order_filled_event.amount, order_filled_event.trade_fee.flat_fees)

    def handle_fill_order(self, order_id: str, fill_price: Optional[Decimal] = None, fill_amount: Optional[Decimal] = None, fees: Optional[List[TokenAmount]] = None):
        if order_id not in self._child_orders:
            self.logger().info(f"received fill for order {order_id} but it's not in child orders")
            return

        child_order = self._child_orders[order_id]
        algo_order_id = child_order.algo_order_id
        if algo_order_id in self._algo_orders:
            algo_order = self._algo_orders[algo_order_id]
            self.logger().info(f"algo order {algo_order.order_id} filled: ")
            if fees is not None:
                self.logger().info(f"flat fee {fees}")
                algo_order.handle_order_fees(fees)
            if fill_price is None:
                fill_price = child_order.price
            if fill_amount is None:
                fill_amount = child_order.unfilled_amount()
            algo_order.handle_order_filled(order_id, fill_price, fill_amount, utils.now_ns())
        else:
            self.logger().error(f"algo order {algo_order_id} not in active algo order")

        if child_order.is_fully_filled():
            self.logger().info(f"child order {child_order.order_id} fully filled, remove tracking")
            del self._child_orders[child_order.order_id]

    def active_positions_df(self) -> pd.DataFrame:
        columns = ["Connector", "Symbol", "Type", "Entry", "Amount", "Leverage"]
        data = []
        for market_pair in self._market_pairs:
            if "perpetual" not in market_pair.market.name:
                continue

            for position in utils.get_positions(market_pair):
                if not position:
                    continue
                data.append(
                    [
                        market_pair.market.name,
                        position.trading_pair,
                        position.position_side.name,
                        position.entry_price,
                        position.amount,
                        position.leverage,
                    ]
                )
        return pd.DataFrame(data=data, columns=columns)

    def wallet_df(self) -> pd.DataFrame:
        data = []
        columns = ["Connector", "Asset", "Bid", "Ask"]

        def get_data(market_pair_tuple) -> List[Any]:
            market_pair, trading_pair = market_pair_tuple, market_pair_tuple.trading_pair
            return [
                market_pair.market.name,
                trading_pair,
                market_pair.get_price(False),
                market_pair.get_price(True),
            ]

        for market_pair_tuple in self._market_pairs:
            data.append(get_data(market_pair_tuple))
        return pd.DataFrame(data=data, columns=columns)

    def format_status(self) -> str:
        def get_constants_str() -> List[str]:
            return ["", "  Constants:"] + ["    " + f"ENV {constants.ENV}"]

        def get_wallet_status_str() -> List[str]:
            wallet_df = self.wallet_balance_data_frame(self._market_pairs)
            return ["", "  Wallet:"] + ["    " + line for line in str(wallet_df).split("\n")]

        def get_asset_status_str() -> List[str]:
            assets_df = self.wallet_df()
            return ["", "  Assets:"] + ["    " + line for line in str(assets_df).split("\n")]

        def get_position_status_str() -> List[str]:
            positions_df = self.active_positions_df()
            if not positions_df.empty:
                return ["", "  Positions:"] + ["    " + line for line in str(positions_df).split("\n")]
            return ["", "  No positions."]

        def get_order_status_str() -> List[str]:
            active_orders = self.order_tracker.active_limit_orders
            if active_orders:
                orders = [order[1] for order in active_orders]
                df = LimitOrder.to_pandas(orders)
                df_lines = str(df).split("\n")
                return ["", "  Active orders:"] + ["    " + line for line in df_lines]
            return ["", "  No active orders."]

        lines = (
            get_constants_str()
            + get_wallet_status_str()
            + get_asset_status_str()
            + get_position_status_str()
            + get_order_status_str()
            + [""]
            + self.network_warning(self._market_pairs)
        )
        return "\n".join(lines)

    def place_single_order(
        self,
        market_pair: MarketTradingPairTuple,
        is_buy: bool,
        price: Decimal,
        amount: Decimal,
        order_type: OrderType = OrderType.LIMIT
    ):
        trade = self.buy_with_specific_market if is_buy else self.sell_with_specific_market
        hedge_order_id = trade(
            market_pair,
            amount,
            order_type=order_type,
            price=price,
            position_action=PositionAction.OPEN,  # this only matters for HEDGE mode
        )
        return hedge_order_id

    def calculate_execute_price(self, price_iter: Iterator, amount: Decimal) -> Optional[Tuple[Decimal, Decimal]]:
        best_price: Optional[Decimal] = None
        total_cost = Decimal('0')
        amount_sofar = Decimal('0')

        hedge_price = None
        while True:
            try:
                price_entry = next(price_iter)
            except StopIteration:
                break

            if best_price is None:
                best_price = price_entry.price

            remaining_amount = amount - amount_sofar
            amount_at_price = min(price_entry.amount, remaining_amount)
            self.logger().info(f"amount_at_price {amount_at_price} price {price_entry.price}")

            total_cost += amount_at_price * price_entry.price
            amount_sofar += amount_at_price

            if amount_sofar >= amount:
                hedge_price = price_entry.price
                break

        if hedge_price is None:
            return None
        if amount_sofar == 0:
            self.logger().error("amount_sofar is 0")
            average_price = hedge_price
        else:
            average_price = total_cost / amount_sofar
        assert best_price is not None

        slippage = (average_price - best_price) / best_price
        return hedge_price, slippage

    async def poll_algo_request(self):
        while True:
            try:
                request = await self._algo_request_queue.get()
                await self.process_algo_request(request)
            except Exception as e:
                self.logger().error(f"unhandled exception thrown in poll order request loop: {e}")
                self.logger().info(traceback.format_exc())

    def is_algo_request_valid(self, request: AlgoRequest) -> bool:
        if request.order_id in self._algo_orders:
            self.logger().error(f"request {request.order_id} already in algo orders")
            return False

        if request.market_pair not in self._market_pairs_map:
            self.logger().error(f"request {request.order_id} market pair {request.market_pair} not found")
            return False

        if request.side not in ["buy", "sell"]:
            self.logger().error(f"request {request.order_id} side not found")
            return False

        match request.algo_type:
            case "BASIC_TWAP":
                if len(request.hedge_market_pair) != 0:
                    self.logger().error(f"request {request.order_id} basic twap does not require hedge market pair ")
                    return False
            case "HEDGE_TWAP":
                if len(request.hedge_market_pair) == 0 or request.hedge_market_pair not in self._market_pairs_map:
                    self.logger().error(f"request {request.order_id} hedge market pair {request.hedge_market_pair} not found")
                    return False
            case _:
                self.logger().error(f"request {request.order_id} algo type {request.algo_type} ")
                return False
        return True

    def create_algo_order(self, request: AlgoRequest) -> Optional[AlgoOrder]:
        algo_order = self._algo_orders.setdefault(request.order_id, AlgoOrder(request=request, start_time=utils.now_ns()))

        success = self._db_manager.add_algo_order(algo_order)
        if not success:
            self.logger().error(f"failed to add algo order {algo_order}")
            return None
        self.logger().info(f"algo order added {algo_order}")

        self.logger().info(
            f"algo order {request.order_id} interval {algo_order.interval / utils.TIME_PRECISION}s interval_amount {algo_order.interval_amount} "
        )
        return algo_order

    def create_algo_child_order(
            self,
            time: int,
            market_pair_name: str,
            algo_order_id: int,
            order_id: str,
            is_hedge: bool,
            is_buy: bool,
            price: Decimal,
            amount: Decimal) -> Optional[AlgoChildOrder]:
        child_order = self._child_orders.setdefault(order_id, AlgoChildOrder(
            time=time,
            market_pair_name=market_pair_name,
            algo_order_id=algo_order_id,
            order_id=order_id,
            is_hedge=is_hedge,
            is_buy=is_buy,
            price=price,
            amount=amount
        ))
        success = self._db_manager.add_algo_child_order(child_order)
        if not success:
            self.logger().error(f"failed to add algo child order {order_id} on market {market_pair_name} for algo {algo_order_id}")
            return None
        self.logger().info(
            f"child order added {order_id} on market {market_pair_name} for algo {algo_order_id}"
        )
        return child_order

    def get_market_pairs(self, request: AlgoRequest) -> Tuple[MarketTradingPairTuple, Optional[MarketTradingPairTuple]]:
        market_pair = self._market_pairs_map[request.market_pair]
        hedge_market_pair = self._market_pairs_map[request.hedge_market_pair] if request.hedge_market_pair in self._market_pairs_map else None
        return market_pair, hedge_market_pair

    def calculate_twap_ratio(self, start_time: int, duration: int, prev_execute_time: int, next_execute_time: int) -> Tuple[Decimal, Decimal]:
        if duration == 0 or prev_execute_time < start_time or next_execute_time < start_time or next_execute_time < prev_execute_time:
            return Decimal(0), Decimal(0)
        floor_amount = Decimal((prev_execute_time - start_time) / duration)
        floor_amount = max(min(floor_amount, Decimal(1)), Decimal(0))
        target_amount = Decimal((next_execute_time - start_time) / duration)
        target_amount = max(min(target_amount, Decimal(1)), Decimal(0))
        return target_amount, floor_amount

    def execute_basic_twap(self, algo_order: AlgoOrder, market_pair: MarketTradingPairTuple) -> bool:
        current_time = utils.now_ns()

        if algo_order.is_canceled():
            self.logger().info(f"algo order {algo_order.order_id} canceled")
            return True

        if algo_order.is_fully_filled():
            self.logger().info(f"algo order {algo_order.order_id} fully filled")
            return True

        if algo_order.is_failed() or algo_order.is_expired(current_time):
            self.logger().info(f"algo order {algo_order.order_id} failed or expired")
            return True

        if current_time > algo_order.next_execute_time:
            algo_order.prev_execute_time = algo_order.next_execute_time
            algo_order.next_execute_time += algo_order.interval

        num_interval = round(algo_order.duration / algo_order.interval)
        interval = int((current_time - algo_order.start_time) / algo_order.interval) + 1
        self.logger().info(f"algo order {algo_order.order_id} interval {interval}/{num_interval}")

        target_ratio, floor_ratio = self.calculate_twap_ratio(
            algo_order.start_time,
            algo_order.duration,
            algo_order.prev_execute_time,
            algo_order.next_execute_time)
        floor_amount = floor_ratio * algo_order.amount
        target_amount = target_ratio * algo_order.amount

        is_buy = algo_order.is_buy
        is_behind = algo_order.amount_filled < floor_amount - constants.AMOUNT_QUANTIZE
        todo_amount = floor_amount if is_behind else target_amount
        target_todo_amount = todo_amount - algo_order.amount_filled - algo_order.amount_filled_pending
        target_todo_amount = target_todo_amount.quantize(Decimal(constants.AMOUNT_QUANTIZE))

        percent_time = (current_time - algo_order.prev_execute_time) / algo_order.interval
        self.logger().info(f"=== {algo_order.algo_type.name} algo order {algo_order.order_id} interval {interval}/{num_interval} pct {percent_time: .2f} floor {floor_amount} target {target_amount}")
        self.logger().info(f"filled {algo_order.amount_filled} todo {target_todo_amount} pending {algo_order.amount_filled_pending}")

        if is_behind or algo_order.percent_floating.is_nan() or percent_time > algo_order.percent_floating:
            if algo_order.is_order_pending():
                self.check_pending_orders(algo_order, current_time, True)
                self.logger().info(f"algo order {algo_order.order_id} waiting for pending orders before crossing")
                return False

            book_iter = market_pair.order_book_ask_entries() if is_buy else market_pair.order_book_bid_entries()
            execute_info = self.calculate_execute_price(book_iter, target_todo_amount)
            if execute_info is None:
                self.logger().info(f"algo order {algo_order.order_id} failed to get execute info todo {target_todo_amount}")
                return False

            order_price, order_slippage = execute_info
            if order_slippage > algo_order.slippage_threshold:
                self.logger().info(f"algo order {algo_order.order_id} slippage {order_slippage} exceeded threshold {algo_order.slippage_threshold}")
                return False
            self.logger().info(f"algo order {algo_order.order_id} crossing market at price {order_price} slippage {order_slippage}")
            intention_str = "cross"
        else:
            order_price = market_pair.get_price(not is_buy)
            order_slippage = Decimal(0)
            if algo_order.pending_order:
                if (abs(order_price - algo_order.pending_order.price) > order_price * Decimal(0.0001)):
                    self.cancel_pending_market(algo_order, current_time)
                    self.logger().info(f"algo order {algo_order.order_id} cancelling order {algo_order.pending_order.order_id}")
                    return False
                else:
                    self.check_pending_orders(algo_order, current_time)
                    self.logger().info(f"algo order {algo_order.order_id} waiting for pending orders before resting")
                    return False
            self.logger().info(f"algo order {algo_order.order_id} resting orders at spot price {order_price}")
            intention_str = "rest"

        if target_todo_amount > 0:
            spot_order_id = self.place_single_order(
                market_pair=market_pair,
                is_buy=is_buy,
                price=order_price,
                amount=target_todo_amount,
                order_type=OrderType.LIMIT)
            child_order = self.create_algo_child_order(
                time=current_time,
                market_pair_name=market_pair.name,
                algo_order_id=algo_order.order_id,
                order_id=spot_order_id,
                is_hedge=False,
                is_buy=is_buy,
                price=order_price,
                amount=target_todo_amount
            )
            assert child_order is not None
            algo_order.handle_order_created(child_order)
            self.logger().info(f"algo order {algo_order.order_id} sending child order {algo_order.pending_order} intent {intention_str}")
        return False

    def execute_hedge_twap(self, algo_order: AlgoOrder, market_pair: MarketTradingPairTuple, hedge_market_pair: MarketTradingPairTuple):
        current_time = utils.now_ns()

        if algo_order.is_failed():
            self.logger().critical(f"algo order {algo_order.order_id} rejected too many times! canceling orders and stopping strategy")
            return True

        if algo_order.is_canceled():
            self.logger().info(f"algo order {algo_order.order_id} done: canceled")
            return True

        if algo_order.is_fully_filled() and algo_order.is_hedged():
            self.logger().info(f"algo order {algo_order.order_id} done: filled and hedged")
            return True

        if algo_order.is_expired(current_time):
            self.logger().info(f"algo order {algo_order.order_id} done: expired")
            return True

        imbalance = abs(algo_order.amount_filled - algo_order.amount_hedged)
        imbalance_threshold = (algo_order.amount / int(algo_order.request.duration_slice)) * constants.IMBALANCE_SLICE_THRESHOLD
        if imbalance > imbalance_threshold:
            self.logger().error(f"algo order {algo_order.order_id} imbalance {imbalance} spot {algo_order.amount_filled} perp {algo_order.amount_hedged}, canceling orders and stopping algo")
            return True

        if current_time > algo_order.next_execute_time:
            algo_order.prev_execute_time = algo_order.next_execute_time
            algo_order.next_execute_time += algo_order.interval

        target_ratio, floor_ratio = self.calculate_twap_ratio(
            algo_order.start_time,
            algo_order.duration,
            algo_order.prev_execute_time,
            algo_order.next_execute_time)

        floor_amount = floor_ratio * algo_order.amount
        floor_amount = floor_amount.quantize(Decimal(constants.AMOUNT_QUANTIZE))
        target_amount = target_ratio * algo_order.amount
        target_amount = target_amount.quantize(Decimal(constants.AMOUNT_QUANTIZE))

        num_interval = round(algo_order.duration / algo_order.interval)
        interval = int((current_time - algo_order.start_time) / algo_order.interval) + 1

        interval_length = algo_order.next_execute_time - algo_order.prev_execute_time
        percent_time = (current_time - algo_order.prev_execute_time) / (interval_length) if interval_length != 0 else 0

        is_buy = algo_order.is_buy
        is_buy_perp = not is_buy
        is_last_interval = algo_order.next_execute_time > algo_order.end_time - interval_length * utils.TIME_PRECISION // 2

        is_spot_behind = algo_order.amount_filled < floor_amount - constants.AMOUNT_QUANTIZE
        is_perp_behind = algo_order.amount_hedged < floor_amount - constants.AMOUNT_QUANTIZE
        is_behind = is_spot_behind or is_perp_behind
        todo_amount = floor_amount if not is_last_interval and is_behind else target_amount

        spot_amount = todo_amount - algo_order.amount_filled - algo_order.amount_filled_pending
        spot_amount = spot_amount.quantize(Decimal(constants.AMOUNT_QUANTIZE))

        perp_amount = todo_amount - algo_order.amount_hedged - algo_order.amount_hedged_pending
        perp_amount = perp_amount.quantize(Decimal(constants.AMOUNT_QUANTIZE))

        self.logger().info(f"=== {algo_order.algo_type.name} algo order {algo_order.order_id} interval {interval}/{num_interval} pct {percent_time: .2f} floor {floor_amount} target {target_amount}")
        self.logger().info(f"filled {algo_order.amount_filled} todo {spot_amount} pending {algo_order.amount_filled_pending}")
        self.logger().info(f"hedged {algo_order.amount_hedged} todo {perp_amount} pending {algo_order.amount_hedged_pending}")

        if is_behind or algo_order.percent_floating.is_nan() or percent_time > algo_order.percent_floating:
            if algo_order.is_order_pending() or algo_order.is_hedge_order_pending():
                self.check_pending_orders(algo_order, current_time, True)
                self.logger().info(f"algo order {algo_order.order_id} waiting for pending orders before crossing")
                return False

            spot_iter = market_pair.order_book_ask_entries() if is_buy else market_pair.order_book_bid_entries()
            spot_info = self.calculate_execute_price(spot_iter, spot_amount)
            if spot_info is None:
                self.logger().info(f"algo order {algo_order.order_id} failed to get execute info todo {spot_amount}")
                return False
            spot_price, spot_slippage = spot_info

            perp_iter = hedge_market_pair.order_book_ask_entries() if is_buy_perp else hedge_market_pair.order_book_bid_entries()
            perp_info = self.calculate_execute_price(perp_iter, perp_amount)
            if perp_info is None:
                self.logger().info(f"algo order {algo_order.order_id} failed to get execute info todo {perp_amount}")
                return False
            perp_price, perp_slippage = perp_info

            self.logger().info(f"algo order {algo_order.order_id} crossing market at spot price {spot_price} slippage {spot_slippage} perp price {perp_price} slippage {perp_slippage}")
            intention_str = "cross"
        else:
            spot_price = market_pair.get_price(not is_buy)
            perp_price = hedge_market_pair.get_price(not is_buy_perp)
            spot_slippage = Decimal(0)
            perp_slippage = Decimal(0)
            if algo_order.pending_order:
                if (abs(spot_price - algo_order.pending_order.price) > spot_price * Decimal(0.0001)):
                    self.cancel_pending_market(algo_order, current_time)
                    self.logger().info(f"algo order {algo_order.order_id} cancelling order {algo_order.pending_order.order_id}")

            if algo_order.pending_hedge_order:
                if (abs(perp_price - algo_order.pending_hedge_order.price) > perp_price * Decimal(0.0001)):
                    self.cancel_pending_hedge_market(algo_order, current_time)
                    self.logger().info(f"algo order {algo_order.order_id} cancelling order {algo_order.pending_hedge_order.order_id}")

            if algo_order.pending_order or algo_order.pending_hedge_order:
                self.check_pending_orders(algo_order, current_time)
                self.logger().info(f"algo order {algo_order.order_id} waiting for pending orders before resting")
                return False

            self.logger().info(f"algo order {algo_order.order_id} resting orders at spot price {spot_price} perp price {perp_price}")
            intention_str = "rest"

        if spot_slippage > algo_order.slippage_threshold or perp_slippage > algo_order.slippage_threshold:
            self.logger().info(f"algo order {algo_order.order_id} slippage exceeded spot {spot_slippage} perp {perp_slippage}")
            return False

        basis = (perp_price - spot_price) / spot_price
        if not is_last_interval and not is_behind and not algo_order.basis_threshold.is_nan():
            if is_buy and basis < algo_order.basis_threshold:
                self.logger().info(f"algo order {algo_order.order_id} not executing since basis {basis} below threshold {algo_order.basis_threshold}")
                return False
            elif not is_buy and basis > algo_order.basis_threshold:
                self.logger().info(f"algo order {algo_order.order_id} not executing since basis {basis} above threshold {algo_order.basis_threshold}")
                return False

        if spot_amount > Decimal(0):
            spot_order_id = self.place_single_order(
                market_pair=market_pair,
                is_buy=is_buy,
                price=spot_price,
                amount=spot_amount,
                order_type=OrderType.LIMIT)
            child_order = self.create_algo_child_order(
                time=current_time,
                market_pair_name=market_pair.name,
                algo_order_id=algo_order.order_id,
                order_id=spot_order_id,
                is_hedge=False,
                is_buy=is_buy,
                price=spot_price,
                amount=spot_amount
            )
            assert child_order is not None
            algo_order.handle_order_created(child_order)
            self.logger().info(f"algo order {algo_order.order_id} sending child order {algo_order.pending_order} intent {intention_str}")

        if perp_amount > Decimal(0):
            perp_order_id = self.place_single_order(
                market_pair=hedge_market_pair,
                is_buy=is_buy_perp,
                price=perp_price,
                amount=perp_amount,
                order_type=OrderType.LIMIT)
            child_order = self.create_algo_child_order(
                time=current_time,
                market_pair_name=hedge_market_pair.name,
                algo_order_id=algo_order.order_id,
                order_id=perp_order_id,
                is_hedge=True,
                is_buy=is_buy,
                price=perp_price,
                amount=perp_amount
            )
            assert child_order is not None
            algo_order.handle_order_created(child_order)
            self.logger().info(f"algo order {algo_order.order_id} sending hedge child order {algo_order.pending_hedge_order} intent {intention_str}")
        return False

    def cancel_pending_market(self, order: AlgoOrder, time: int):
        if order.pending_order:
            self.logger().info(f"algo order {order.order_id} cancelling spot order {order.pending_order.order_id}")
            market_pair = self._market_pairs_map[order.pending_order.market_pair_name]
            order.pending_order.last_cancel_time = time
            self.cancel_order(market_pair, order.pending_order.order_id)

    def cancel_pending_hedge_market(self, order: AlgoOrder, time: int):
        if order.pending_hedge_order:
            self.logger().info(f"algo order {order.order_id} cancelling hedge order {order.pending_hedge_order.order_id}")
            market_pair = self._market_pairs_map[order.pending_hedge_order.market_pair_name]
            order.pending_hedge_order.last_cancel_time = time
            self.cancel_order(market_pair, order.pending_hedge_order.order_id)

    def check_pending_orders(self, order: AlgoOrder, time: int, immediate_cancel: bool = False):
        if order.pending_order:
            if immediate_cancel or time - order.pending_order.time > constants.CHILD_ORDER_EXPIRE * utils.TIME_PRECISION:
                if (order.pending_order.last_cancel_time == 0 or
                        time - order.pending_order.last_cancel_time > constants.CHILD_ORDER_CANCEL_INTERVAL * utils.TIME_PRECISION):
                    self.logger().info(f"algo order {order.order_id} child order {order.pending_order.order_id} expired, sending cancel")
                    market_pair = self._market_pairs_map[order.pending_order.market_pair_name]
                    order.pending_order.last_cancel_time = time
                    self.cancel_order(market_pair, order.pending_order.order_id)

        if order.pending_hedge_order:
            if immediate_cancel or time - order.pending_hedge_order.time > constants.CHILD_ORDER_EXPIRE * utils.TIME_PRECISION:
                if (order.pending_hedge_order.last_cancel_time == 0 or
                        time - order.pending_hedge_order.last_cancel_time > constants.CHILD_ORDER_CANCEL_INTERVAL * utils.TIME_PRECISION):
                    self.logger().info(f"algo order {order.order_id} child hedge order {order.pending_hedge_order.order_id} expired, sending cancel")
                    market_pair = self._market_pairs_map[order.pending_hedge_order.market_pair_name]
                    order.pending_hedge_order.last_cancel_time = time
                    self.cancel_order(market_pair, order.pending_hedge_order.order_id)

    def is_balance_available(self, request: AlgoRequest, market_pair: MarketTradingPairTuple) -> bool:
        is_buy = request.side == "buy"
        is_spot = "perpetual" not in market_pair.market.name
        mid_price = market_pair.get_mid_price()
        algo_base_amount = Decimal(request.amount)
        if is_spot:
            base_balance = market_pair.market.get_available_balance(market_pair.base_asset)
            quote_balance = market_pair.market.get_available_balance(market_pair.quote_asset)
            self.logger().info(f"algo request {request.order_id} spot base {base_balance} quote {quote_balance} mid price {mid_price} algo base {algo_base_amount}")
            if is_buy:
                quote_balance_base = quote_balance / mid_price
                if algo_base_amount > quote_balance_base:
                    self.logger().error(f"algo request {request.order_id} insufficient quote balance {quote_balance} to buy {algo_base_amount}")
                    return False
            else:
                if algo_base_amount > base_balance:
                    self.logger().error(f"algo request {request.order_id} insufficient base balance {base_balance} to sell {algo_base_amount}")
                    return False
        else:  # perp
            leverage = self._config_map.execute_leverage
            margin_balance = market_pair.market.account_margin_balance
            self.logger().info(f"algo request {request.order_id} perp margin {margin_balance} mid price {mid_price} algo base {algo_base_amount}")
            algo_margin_needed = algo_base_amount * mid_price / leverage
            if algo_margin_needed > margin_balance:
                self.logger().error(f"algo request {request.order_id} insufficient margin balance {margin_balance} to execute {algo_margin_needed}")
                return False
        return True

    async def process_algo_request(self, request: AlgoRequest):
        if not self.is_algo_request_valid(request):
            self.logger().info(f"algo request {request.order_id} invalid, skip processing")
            return
        market_pair, hedge_market_pair = self.get_market_pairs(request)
        if request.algo_type == "BASIC_TWAP":
            if not self.is_balance_available(request, market_pair):
                self.logger().info(f"algo request {request.order_id} insufficient balance, skip processing")
                return
        elif request.algo_type == "HEDGE_TWAP":
            assert hedge_market_pair is not None
            if not self.is_balance_available(request, market_pair) or not self.is_balance_available(request, hedge_market_pair):
                self.logger().info(f"algo request {request.order_id} insufficient balance, skip processing")
                return
        else:
            assert False, f"invalid algo type {request.algo_type}"
        algo_order = self.create_algo_order(request)
        if algo_order is None:
            self.logger().info(f"algo request {request.order_id} failed to create, skip processing")
            return
        while True:
            match algo_order.algo_type:
                case AlgoType.BASIC_TWAP:
                    assert hedge_market_pair is None
                    done = self.execute_basic_twap(algo_order, market_pair)
                case AlgoType.HEDGE_TWAP:
                    assert hedge_market_pair is not None
                    done = self.execute_hedge_twap(algo_order, market_pair, hedge_market_pair)
                case _:
                    raise ValueError(f"unknown algo type {algo_order.algo_type}")
            if done:
                break
            await asyncio.sleep(constants.CHECK_ALGO_INTERVAL)

        while algo_order.is_order_pending() or algo_order.is_hedge_order_pending():
            self.logger().info(f"algo order {algo_order.order_id} has pending orders")
            self.check_pending_orders(algo_order, utils.now_ns(), True)
            await asyncio.sleep(constants.CHILD_ORDER_CANCEL_INTERVAL)

        self.logger().info(f"removing algo order {algo_order.order_id}")
        self.logger().info(
            f"filled {algo_order.amount_filled} filled price {algo_order.avg_fill_price()} "
            f"hedged {algo_order.amount_hedged} hedged price {algo_order.avg_hedge_price()} fees {algo_order.fees}")
        del self._algo_orders[algo_order.request.order_id]

    async def handle_algo_request(self, algo_request: AlgoRequest):
        if len(self._algo_orders) != 0:
            self.logger().info(f"algo order in progress, rejecting {algo_request.order_id}")
            return {"success": False}

        self.logger().info(f"received algo order {algo_request.order_id}")
        await self._algo_request_queue.put(algo_request)
        return {"success": True, "order_id": algo_request.order_id}

    async def handle_cancel_request(self, algo_order_id: int):
        if len(self._algo_orders) == 0 or algo_order_id not in self._algo_orders:
            return {"success": False, "error": f"algo order {algo_order_id} is not active"}

        self.logger().info(f"cancelling algo order {algo_order_id}")
        self._algo_orders[algo_order_id].canceled = True
        return {"success": True, "order_id": algo_order_id}

    async def handle_status_request(self):
        self.logger().info("receive status request")
        response = {"success": True, "timestamp": int(utils.now())}
        wallet_df = self.wallet_balance_data_frame(self._market_pairs)
        response["wallets"] = wallet_df.to_dict("records")
        assets_df = self.wallet_df()
        response["assets"] = assets_df.to_dict("records")
        positions_df = self.active_positions_df()
        response["positions"] = positions_df.to_dict("records")
        active_orders = self.order_tracker.active_limit_orders
        if active_orders:
            df = LimitOrder.to_pandas(active_orders)
            response["active_orders"] = df.to_dict("records")
        response["active_algo_order"] = list(self._algo_orders.keys())
        return response
