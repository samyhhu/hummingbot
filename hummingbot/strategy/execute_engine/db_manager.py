import logging

import sqlalchemy

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.logger.logger import HummingbotLogger
from hummingbot.model.algo_child_order import AlgoChildOrder as DbAlgoChildOrder
from hummingbot.model.algo_order import AlgoOrder as DbAlgoOrder
from hummingbot.model.sql_connection_manager import SQLConnectionManager
from hummingbot.strategy.execute_engine.data_types import AlgoChildOrder, AlgoOrder


class DbManager:
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    def __init__(self, client_config_map: ClientConfigMap, db_name=None):
        self._sql_manager = SQLConnectionManager.get_trade_fills_instance(client_config_map, db_name)

    @staticmethod
    def to_algo_order(order: AlgoOrder) -> DbAlgoOrder:
        return DbAlgoOrder(
            order_id=order.order_id,
            timestamp=order.start_time,
            pair=order.request.market_pair,
            hedge_pair=order.request.hedge_market_pair,
            side=order.request.side,
            amount=order.request.amount,
            basis_threshold=order.request.basis_threshold,
            slippage_threshold=order.request.slippage_threshold,
            duration=order.request.duration,
            duration_slice=order.request.duration_slice,
        )

    @staticmethod
    def to_algo_child_order(order: AlgoChildOrder) -> DbAlgoChildOrder:
        return DbAlgoChildOrder(
            timestamp=order.time,
            pair=order.market_pair_name,
            algo_order_id=order.algo_order_id,
            order_id=order.order_id,
        )

    def add_algo_order(self, order: AlgoOrder):
        try:
            with self._sql_manager.get_new_session() as session:
                with session.begin():
                    algo_order = self.to_algo_order(order)
                    session.add(algo_order)
        except sqlalchemy.exc.IntegrityError as e:
            self.logger().error(f"failed to add order: integraty error {e}")
            return False
        except Exception as e:
            self.logger().error(f"unhandle exception in add order: {e}")
            return False
        return True

    def add_algo_child_order(self, order: AlgoChildOrder):
        try:
            with self._sql_manager.get_new_session() as session:
                with session.begin():
                    child_order = self.to_algo_child_order(order)
                    session.add(child_order)
        except sqlalchemy.exc.IntegrityError as e:
            self.logger().error(f"failed to add child order: integraty error {e}")
            return False
        except Exception as e:
            self.logger().error(f"unhandle exception in add child order: {e}")
            return False
        return True
