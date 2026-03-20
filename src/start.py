import logging
from config import setup_logging
import simple_api



def main():
    setup_logging()
    logger = logging.getLogger("crawler.start")

    server = simple_api.HTTPServer((simple_api.HOST, simple_api.PORT), simple_api.ApiHandler)
    logger.info("API listening on http://%s:%s/status", simple_api.HOST, simple_api.PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping API server...")
        server.server_close()
    except Exception:
        logger.exception("API server crashed")
        server.server_close()
        raise

if __name__ == "__main__":
    main()