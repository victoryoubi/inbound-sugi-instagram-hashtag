import os
from src.refresh import run_refresh
from src.collect import run_collect

if __name__ == "__main__":
    mode = os.getenv("MODE", "").strip().lower()

    if mode == "refresh":
        run_refresh()
    elif mode == "hashtag":
        run_hashtag()
    elif mode == "collect":
        run_collect()
    else:
        raise ValueError("MODE must be set to 'refresh' or 'collect'")
