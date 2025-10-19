from hummingbot.strategy.execute_engine.execute_engine import ExecuteEngineStrategy
from hummingbot.strategy.execute_engine.execute_engine_config_map_pydantic import MAX_CONNECTOR, ExecuteEngineConfigMap
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


async def start(self):
    c_map: ExecuteEngineConfigMap = self.strategy_config_map
    initialize_markets_list = []
    for i in range(MAX_CONNECTOR):
        connector_config = getattr(c_map, f"execute_connector_{i}")
        connector = connector_config.connector
        if not connector:
            continue
        connector = connector.lower()
        market = connector_config.market
        initialize_markets_list.append((connector, [market]))

    await self.initialize_markets(initialize_markets_list)
    self.market_trading_pair_tuples = []
    for connector, markets in initialize_markets_list:
        assert len(markets) == 1
        market = markets[0]
        base, quote = market.split("-")
        market_info = MarketTradingPairTuple(
            self.markets[connector], market, base, quote
        )
        self.market_trading_pair_tuples.append(market_info)

    database_name = self.strategy_file_name.split(".")[0]
    self.strategy = ExecuteEngineStrategy(
        config_map=c_map,
        market_pairs=self.market_trading_pair_tuples,
        client_config_map=self.client_config_map,
        database_name=database_name,
    )
