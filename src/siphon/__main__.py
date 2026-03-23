"""Entry point for Siphon."""
import argparse
import logging
import sys

import uvicorn

from siphon.config import load_config


def main():
    parser = argparse.ArgumentParser(description="Siphon — YouTube to Podcast bridge")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--no-tray",
        action="store_true",
        help="Disable system tray icon",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        config = load_config(args.config)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Set below-normal process priority on Windows for gaming friendliness
    try:
        import os
        if sys.platform == "win32":
            import ctypes
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            PROCESS_ALL_ACCESS = 0x001F0FFF
            pid = os.getpid()
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, pid)
            if handle:
                ctypes.windll.kernel32.SetPriorityClass(handle, BELOW_NORMAL_PRIORITY_CLASS)
                ctypes.windll.kernel32.CloseHandle(handle)
                logging.info("Set process to below-normal priority")
    except Exception:
        pass

    # Start system tray icon (background thread)
    if not args.no_tray:
        try:
            from siphon.tray import SiphonTray
            tray = SiphonTray(port=config.server.port, host="127.0.0.1")
            tray.run_in_background()
            logging.info("System tray icon started")
        except Exception as e:
            logging.warning(f"System tray failed to start: {e}")

    from siphon.app import create_app
    app = create_app(config)

    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()
