import os

def main():
    token = os.environ.get("LONG_TOKEN", "")
    print("LONG_TOKEN length:", len(token))
    if not token:
        raise RuntimeError("LONG_TOKEN is empty. Check Secret Manager binding.")

if __name__ == "__main__":
    main()
