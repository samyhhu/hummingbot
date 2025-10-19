from typing import Any, Dict

from sqlalchemy import BigInteger, Column, ForeignKey, Index, Integer, Text

from hummingbot.model import HummingbotBase


class AlgoChildOrder(HummingbotBase):
    __tablename__ = "AlgoChildOrder"
    __table_args__ = (Index("daco_pair_timestamp_index", "pair", "timestamp")),

    id = Column(Integer, primary_key=True, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    pair = Column(Text, nullable=False)
    algo_order_id = Column(BigInteger, ForeignKey("AlgoOrder.order_id"), nullable=False)
    order_id = Column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"AlgoChildOrder(id={self.id}, order_id={self.order_id}, timestamp={self.timestamp}, pair='{self.pair}', " \
               f"algo_order_id='{self.algo_order_id}', order_id='{self.order_id}')"

    @staticmethod
    def to_bounty_api_json(order: "AlgoChildOrder") -> Dict[str, Any]:
        return {
            "timestamp": order.timestamp,
            "pair": order.pair,
            "algo_order_id": order.algo_order_id,
            "order_id": order.order_id,
        }
