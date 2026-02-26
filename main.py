import os
from src.refresh import run_refresh
from src.hashtag import run_hashtag
from src.collect import run_collect
from src.extract import run_extract

if __name__ == "__main__":
    mode = os.getenv("MODE", "").strip().lower()

    if mode == "refresh":
        run_refresh()
    elif mode == "hashtag":
        run_hashtag()
    elif mode == "extract":
        run_extract()
    elif mode == "collect":
        run_collect()
    else:
        raise ValueError("MODE must be set to 'refresh' or 'hashtag' or 'extract' or 'collect'")
