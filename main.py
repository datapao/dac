import os
import json
import logging
import argparse

from db import create_db
from scraping import scrape, start_scheduled_scraping


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, help='command to run',
                        choices=["create_db", "scrape", "scrape_once"])
    args = parser.parse_args()
    configpath = os.getenv('DAC_CONFIG_PATH')

    with open(configpath, 'r') as config_file:
        config = json.load(config_file)

    return args.command, config, configpath


if __name__ == "__main__":
    logformat = "%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s"
    logging.basicConfig(level=logging.DEBUG, format=logformat)
    log = logging.getLogger("dac")
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

    command, config, configpath = parse_args()

    log.info(f"Command: {command}")
    log.debug(f"Config loaded from: {configpath}")

    if command == "scrape":
        interval = float(config["scraper"].get("interval"))
        thread = start_scheduled_scraping(interval, configpath)
    elif command == "create_db":
        create_db()
    elif command == "scrape_once":
        scrape(configpath)
