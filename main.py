"""
    Main entrypoint for the FastAPI application
"""

import uvicorn


class App:
    """
        FastAPI App placeholder
    """


app = App()

if __name__ == "__main__":
    uvicorn.run("src.server:APP", host="0.0.0.0", port=4000, log_level="info", workers=1)
