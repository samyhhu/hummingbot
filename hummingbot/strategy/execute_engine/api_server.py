import asyncio

from fastapi import APIRouter, FastAPI
from uvicorn import Config, Server

from hummingbot.strategy.execute_engine.data_types import AlgoRequest
from hummingbot.strategy.execute_engine.log_conf import LOGGING_CONFIG


class ApiHandler:
    def __init__(self):
        self._handler = None
        self._router = APIRouter()

        # Execution endpoints
        self._router.add_api_route("/algo_request", self.handle_algo_request, methods=["POST"])
        self._router.add_api_route("/cancel_request", self.handle_cancel_request, methods=["POST"])
        self._router.add_api_route("/status_request", self.handle_status_request, methods=["POST"])

    def set_handler(self, handler):
        self._handler = handler

    # Execution endpoints
    async def handle_algo_request(self, request: AlgoRequest):
        if not self._handler:
            return {"success": False}
        return await self._handler.handle_algo_request(request)

    async def handle_cancel_request(self, algo_order_id: int):
        if not self._handler:
            return {"success": False}
        return await self._handler.handle_cancel_request(algo_order_id)

    async def handle_status_request(self):
        if not self._handler:
            return {"success": False}
        return await self._handler.handle_status_request()


app = FastAPI(title=" Execution & Monitoring API", version="1.0.0")
api_handler = ApiHandler()


def create_server(handler):
    api_handler.set_handler(handler)
    app.include_router(api_handler._router)
    loop = asyncio.get_event_loop()
    config = Config(app=app, host="0.0.0.0", port=8001, loop=loop, log_config=LOGGING_CONFIG)
    return Server(config)
