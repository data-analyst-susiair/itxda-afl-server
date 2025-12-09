import sys
import os
import uvicorn


# Add the project root to the python path so we can import src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def main():
    print("Starting ITXDA Pipeline API Server...")
    # Run the FastAPI app using uvicorn
    # reload=True is useful for development
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
