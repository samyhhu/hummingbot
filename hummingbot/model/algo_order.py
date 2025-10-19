from typing import Any, Dict

from sqlalchemy import BigInteger, Column, Index, Text

from hummingbot.model import HummingbotBase


class AlgoOrder(HummingbotBase):
    __tablename__ = "AlgoOrder"
    __table_args__ = (Index("dao_pair_timestamp_index", "pair", "timestamp"), Index("dao_hedge_pair_timestamp_index", "hedge_pair", "timestamp"))

    order_id = Column(BigInteger, primary_key=True, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    pair = Column(Text, nullable=False)
    hedge_pair = Column(Text, nullable=False)
    side = Column(Text, nullable=False)
    amount = Column(Text, nullable=False)
    basis_threshold = Column(Text, nullable=False)
    slippage_threshold = Column(Text, nullable=False)
    duration = Column(Text, nullable=False)
    duration_slice = Column(Text, nullable=False)

    def __repr__(self) -> str:
        return f"AlgoOrder(order_id={self.order_id}, timestamp={self.timestamp}, pair='{self.pair}', " \
               f"hedge_pair='{self.hedge_pair}', side={self.side}, amount='{self.amount}', " \
               f"basis_threshold='{self.basis_threshold}', slippage_threshold='{self.slippage_threshold}', "\
               f"duration='{self.duration}', duration_slice='{self.duration_slice}')"

    @staticmethod
    def to_bounty_api_json(order: "AlgoOrder") -> Dict[str, Any]:
        return {
            "order_id": order.order_id,
            "timestamp": order.timestamp,
            "pair": order.pair,
            "hedge_pair": order.hedge_pair,
            "side": order.side,
            "amount": order.amount,
            "basis_threshold": order.basis_threshold,
            "slippage_threshold": order.slippage_threshold,
            "duration": order.duration,
            "duration_slice": order.duration_slice,
        }
