from pathlib import Path

import yaml

_CONFIGS_DIR = Path(__file__).parent


def load_alpha_configs() -> list[dict]:
    with open(_CONFIGS_DIR / "alpha_configs.yml") as f:
        raw = yaml.safe_load(f)
    return [
        {
            "name": c["name"],
            "signal": c["signal"],
            "parameters": {k: v for d in c["parameters"] for k, v in d.items()},
        }
        for c in raw["configs"]
    ]


def load_risk_model_configs() -> list[dict]:
    with open(_CONFIGS_DIR / "risk_model_configs.yml") as f:
        raw = yaml.safe_load(f)
    return [
        {
            "name": c["name"],
            "type": c["type"],
            "factors": c["factors"],
            "window": c["window"],
            "half_life": c["half-life"],
        }
        for c in raw["configs"]
    ]


def load_portfolio_configs() -> list[dict]:
    with open(_CONFIGS_DIR / "portfolio_configs.yml") as f:
        raw = yaml.safe_load(f)
    return [
        {
            "name": c["name"],
            "alpha": c["alpha"],
            "risk_model": c["risk_model"],
            "objective": c["objective"],
            "parameters": {k.replace("-", "_"): v for d in c["parameters"] for k, v in d.items()},
            "constraints": c["constraints"],
        }
        for c in raw["configs"]
    ]
