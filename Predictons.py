import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'streamlit_app'))

from Home import main

if __name__ == "__main__":
    main()
