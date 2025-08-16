import random

import pytest


@pytest.fixture(autouse=True)
def seed() -> int:
    seed = int("0xDEADBEEF", 16)
    random.seed(seed)

    return seed
