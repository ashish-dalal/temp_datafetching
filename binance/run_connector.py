import asyncio
import json
import logging
import websockets
from qdb import QuestDBClient, db_writer, print_mark_price, print_ticker, print_agg_trades, print_book_ticker, print_klines
import time
from binance_connector import BinanceConnector
import os




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics file to share with monitor.py
METRICS_FILE = "metrics.json"

def update_metrics(stream_type: str, received: int, queue_size: int):
    """Update metrics in a JSON file"""
    try:
        metrics = {}
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE, 'r') as f:
                metrics = json.load(f)
        
        if stream_type not in metrics:
            metrics[stream_type] = {'received_count': 0, 'queue_size': 0, 'last_update': 0}
        
        metrics[stream_type]['received_count'] = received
        metrics[stream_type]['queue_size'] = queue_size
        metrics[stream_type]['last_update'] = time.time()
        
        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f, indent=2)
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")

async def main():
    queue = asyncio.Queue(maxsize=1000)
    qdb_client = QuestDBClient()
    
    # List of symbols
    symbols = [
        "btcusdt", "ethusdt", "bchusdt", "xrpusdt", "eosusdt", "ltcusdt", "trxusdt", "etcusdt", "linkusdt",
        "xlmusdt", "adausdt", "xmrusdt", "dashusdt", "zecusdt", "xtzusdt", "bnbusdt", "atomusdt", "ontusdt",
        "iotausdt", "batusdt", "vetusdt", "neousdt", "qtumusdt", "iostusdt", "thetausdt", "algousdt", "zilusdt",
        "kncusdt", "zrxusdt", "compusdt", "dogeusdt", "sxpusdt", "kavausdt", "bandusdt", "rlcusdt", "mkrusdt",
        "snxusdt", "dotusdt", "defiusdt", "yfiusdt", "crvusdt", "trbusdt", "runeusdt", "sushiusdt", "egldusdt",
        "solusdt", "icxusdt", "storjusdt", "uniusdt", "avaxusdt", "enjusdt", "flmusdt", "ksmusdt", "nearusdt",
        "aaveusdt", "filusdt", "rsrusdt", "lrcusdt", "belusdt", "axsusdt", "alphausdt", "zenusdt", "sklusdt",
        "grtusdt", "1inchusdt", "chzusdt", "sandusdt", "ankrusdt", "rvnusdt", "sfpusdt", "cotiusdt", "chrusdt",
        "manausdt", "aliceusdt", "hbarusdt", "oneusdt", "dentusdt", "celrusdt", "hotusdt", "mtlusdt", "ognusdt",
        "nknusdt", "1000shibusdt", "bakeusdt", "gtcusdt", "btcdomusdt", "iotxusdt", "c98usdt", "maskusdt",
        "atausdt", "dydxusdt", "1000xecusdt", "galausdt", "celousdt", "arusdt", "arpausdt", "ctsiusdt",
        "lptusdt", "ensusdt", "peopleusdt", "roseusdt", "duskusdt", "flowusdt", "imxusdt", "api3usdt",
        "gmtusdt", "apeusdt", "woousdt", "jasmyusdt", "opusdt", "injusdt", "stgusdt", "spellusdt",
        "1000luncusdt", "luna2usdt", "ldousdt", "icpusdt", "aptusdt", "qntusdt", "fetusdt", "fxsusdt",
        "hookusdt", "magicusdt", "tusdt", "highusdt", "minausdt", "astrusdt", "phbusdt", "gmxusdt", "cfxusdt",
        "stxusdt", "achusdt", "ssvusdt", "ckbusdt", "perpusdt", "truusdt", "lqtyusdt", "usdcusdt", "idusdt",
        "arbusdt", "joeusdt", "tlmusdt", "leverusdt", "rdntusdt", "hftusdt", "xvsusdt", "ethbtc", "blurusdt",
        "eduusdt", "suiusdt", "1000pepeusdt", "1000flokiusdt", "umausdt", "nmrusdt", "mavusdt", "xvgusdt",
        "wldusdt", "pendleusdt", "arkmusdt", "agldusdt", "yggusdt", "dodoxusdt", "bntusdt", "oxtusdt",
        "seiusdt", "cyberusdt", "hifiusdt", "arkusdt", "bicousdt", "bigtimeusdt", "waxpusdt", "bsvusdt",
        "rifusdt", "polyxusdt", "gasusdt", "powrusdt", "tiausdt", "cakeusdt", "memeusdt", "twtusdt",
        "tokenusdt", "ordiusdt", "steemusdt", "ilvusdt", "ntrnusdt", "kasusdt", "beamxusdt", "1000bonkusdt",
        "pythusdt", "superusdt", "ustcusdt", "ongusdt", "ethwusdt", "jtousdt", "1000satsusdt", "auctionusdt",
        "1000ratsusdt", "aceusdt", "movrusdt", "nfpusdt", "btcusdc", "ethusdc", "bnbusdc", "solusdc",
        "xrpusdc", "aiusdt", "xaiusdt", "dogeusdc", "wifusdt", "mantausdt", "ondousdt", "lskusdt", "altusdt",
        "jupusdt", "zetausdt", "roninusdt", "dymusdt", "suiusdc", "omusdt", "linkusdc", "pixelusdt",
        "strkusdt", "ordiusdc", "glmusdt", "portalusdt", "tonusdt", "axlusdt", "myrousdt", "1000pepeusdc",
        "metisusdt", "aevousdt", "wldusdc", "vanryusdt", "bomeusdt", "ethfiusdt", "avaxusdc", "1000shibusdc",
        "enausdt", "wusdt", "wifusdc", "bchusdc", "tnsrusdt", "sagausdt", "ltcusdc", "nearusdc", "taousdt",
        "omniusdt", "arbusdc", "neousdc", "filusdc", "tiausdc", "bomeusdc", "rezusdt", "enausdc", "ethfiusdc",
        "1000bonkusdc", "bbusdt", "notusdt", "turbousdt", "iousdt", "zkusdt", "mewusdt", "listausdt",
        "zrousdt", "crvusdc", "renderusdt", "bananausdt", "rareusdt", "gusdt", "synusdt", "sysusdt",
        "voxelusdt", "brettusdt", "alpacausdt", "popcatusdt", "sunusdt", "dogsusdt", "mboxusdt", "chessusdt",
        "fluxusdt", "bswusdt", "quickusdt", "neiroethusdt", "rplusdt", "polusdt", "uxlinkusdt",
        "1mbabydogeusdt", "neirousdt", "kdausdt", "fidausdt", "fiousdt", "catiusdt", "ghstusdt", "lokausdt",
        "hmstrusdt", "reiusdt", "cosusdt", "eigenusdt", "diausdt", "1000catusdt", "scrusdt", "goatusdt",
        "moodengusdt", "safeusdt", "santosusdt", "ponkeusdt", "cowusdt", "cetususdt", "1000000mogusdt",
        "grassusdt", "driftusdt", "swellusdt", "actusdt", "pnutusdt", "hippousdt", "1000xusdt", "degenusdt",
        "banusdt", "aktusdt", "slerfusdt", "scrtusdt", "1000cheemsusdt", "1000whyusdt", "theusdt",
        "morphousdt", "chillguyusdt", "kaiausdt", "aerousdt", "acxusdt", "orcausdt", "moveusdt",
        "raysolusdt", "komausdt", "virtualusdt", "spxusdt", "meusdt", "avausdt", "degousdt",
        "velodromeusdt", "mocausdt", "vanausdt", "penguusdt", "lumiausdt", "usualusdt", "aixbtusdt",
        "fartcoinusdt", "kmnousdt", "cgptusdt", "hiveusdt", "dexeusdt", "phausdt", "dfusdt", "griffainusdt",
        "ai16zusdt", "zerebrousdt", "biousdt", "cookieusdt", "alchusdt", "swarmsusdt", "sonicusdt", "dusdt",
        "promusdt", "susdt", "solvusdt", "arcusdt", "avaaiusdt", "trumpusdt", "melaniausdt", "vthousdt",
        "animeusdt", "vineusdt", "pippinusdt", "vvvusdt", "berausdt", "tstusdt", "layerusdt", "heiusdt",
        "b3usdt", "ipusdt", "gpsusdt", "shellusdt", "kaitousdt", "kaitousdc", "ipusdc", "trumpusdc",
        "adausdc", "redusdt", "pnutusdc", "hbarusdc", "vicusdt", "epicusdt", "bmtusdt", "mubarakusdt",
        "formusdt", "bidusdt", "tutusdt", "broccoli714usdt", "broccolif3busdt", "sirenusdt", "bananas31usdt",
        "brusdt", "plumeusdt", "nilusdt", "partiusdt", "jellyjellyusdt", "maviausdt", "paxgusdt", "walusdt",
        "funusdt", "mlnusdt", "gunusdt", "athusdt", "babyusdt", "forthusdt", "promptusdt", "xcnusdt",
        "pumpusdt", "stousdt", "fheusdt", "kernelusdt", "wctusdt", "initusdt", "aergousdt", "bankusdt",
        "eptusdt", "deepusdt"
    ]
    
    # Create connector with all symbols
    connector = BinanceConnector(queue, symbols=symbols)
    
    # Patch BinanceConnector to update metrics
    original_handle_message = connector._handle_message
    async def patched_handle_message(message: str):
        try:
            msg = json.loads(message)
            if "stream" in msg and "data" in msg:
                stream = msg["stream"]
                stream_type = None
                if "markPrice" in stream:
                    stream_type = "markPrice"
                elif "ticker" in stream:
                    stream_type = "ticker"
                elif "aggTrade" in stream:
                    stream_type = "aggTrade"
                elif "bookTicker" in stream:
                    stream_type = "bookTicker"
                elif "kline" in stream:
                    stream_type = "kline"
                if stream_type:
                    # Update metrics with received count and queue size
                    metrics = {}
                    if os.path.exists(METRICS_FILE):
                        with open(METRICS_FILE, 'r') as f:
                            metrics = json.load(f)
                    received_count = metrics.get(stream_type, {}).get('received_count', 0) + 1
                    update_metrics(stream_type, received_count, queue.qsize())
        except Exception as e:
            logger.error(f"Error in patched handle_message: {e}")
        await original_handle_message(message)
    
    connector._handle_message = patched_handle_message
    
    # Register all callbacks
    callbacks = [
        print_mark_price,
        print_ticker,
        print_agg_trades,
        print_book_ticker,
        print_klines
    ]
    
    # Print registered callbacks
    print("Registered Callbacks:")
    for callback in callbacks:
        print(f"- {callback.__name__}")
    
    # Add callbacks to connector
    for callback in callbacks:
        connector.add_callback(callback)
    
    # Start the connector
    connector.start()
    
    # Start WebSocket connection task
    connector_task = asyncio.create_task(connector.connect())
    
    # Try connecting to QuestDB with retry
    max_retries = 5
    retry_count = 0
    db_connected = False
    db_writer_task = None
    
    try:
        # Try to connect to QuestDB
        while retry_count < max_retries and not db_connected:
            try:
                await qdb_client.connect()
                logger.info("Successfully connected to QuestDB")
                db_connected = True
            except Exception as e:
                retry_count += 1
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.error(f"QuestDB connection failed (attempt {retry_count}/{max_retries}): {e}")
                
                if retry_count < max_retries:
                    logger.info(f"Retrying connection in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning("Max retries reached. Running without database storage.")
        
        # Start db_writer task only if connected
        if db_connected:
            db_writer_task = asyncio.create_task(
                db_writer(queue, qdb_client, batch_size=100, batch_timeout=0.5)
            )
            logger.info("Database writer task started")
            
            # Create tasks list with all tasks
            tasks = [connector_task, db_writer_task]
        else:
            # Run only with the connector task
            tasks = [connector_task]
            logger.warning("Running without database storage - only displaying data")
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        # Clean up
        connector.stop()
        if db_connected:
            try:
                await qdb_client.close()
                logger.info("Successfully closed QuestDB connection")
            except Exception as e:
                logger.error(f"Error closing QuestDB connection: {e}")
        # Clean up metrics file
        if os.path.exists(METRICS_FILE):
            os.remove(METRICS_FILE)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())