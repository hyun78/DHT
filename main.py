import asyncio
import dht
import logging
logging.getLogger().setLevel("INFO")


def main():
    loop = asyncio.new_event_loop()

    dht.DHT(loop)

    try:
        loop.run_forever()
    finally:
        loop.close()
    pass

if __name__ == "__main__":
    main()