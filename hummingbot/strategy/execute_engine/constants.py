import os
from decimal import Decimal

from dotenv import load_dotenv

load_dotenv("conf/.env", override=True)

ENV = os.environ.get("ENV", "testnet")

CHECK_ALGO_INTERVAL = 1  # second
CHILD_ORDER_CANCEL_INTERVAL = 5  # second
CHILD_ORDER_EXPIRE = 10  # second
ALLOWED_FAILURES = 3
IMBALANCE_SLICE_THRESHOLD = 3
FLOATING_INTERVAL = 0.95


PRICE_QUANTIZE = Decimal("0.01")
AMOUNT_QUANTIZE = Decimal("0.001")
