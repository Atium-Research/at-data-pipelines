from .momentum import compute as momentum
from .reversal import compute as reversal

REGISTRY = {
    "momentum": momentum,
    "reversal": reversal,
}
