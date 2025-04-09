from extractor import CoinDataExtractor

def test_get_last_load_returns_string():
    coin_extractor = CoinDataExtractor("bitcoin")
    result = coin_extractor.get_last_load()

    if result is None:
        raise ValueError("get_last_load() returned None")

    if not isinstance(result, str):
        raise TypeError("get_last_load() did not return a string")

    if not result.strip():
        raise ValueError("get_last_load() returned empty string")
