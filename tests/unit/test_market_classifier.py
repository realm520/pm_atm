from weather_arb.market_classifier import classify_premarket_market, is_premarket_eligible, PremarketType


def test_classifier_fdv_airdrop_token_launch() -> None:
    assert classify_premarket_market("Will MegaETH FDV exceed $20B?") == PremarketType.FDV
    assert classify_premarket_market("Will project X airdrop before June?") == PremarketType.AIRDROP
    assert classify_premarket_market("Will token launch this quarter?") == PremarketType.TOKEN_LAUNCH


def test_classifier_eligibility() -> None:
    assert is_premarket_eligible("FDV above 10B?") is True
    assert is_premarket_eligible("Airdrop by March?") is True
    assert is_premarket_eligible("Token launch by March?") is False
