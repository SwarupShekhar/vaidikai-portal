import os
import sys

# Ensure the directory of this file is in the Python path
sys.path.append(os.path.dirname(__file__))

from label_studio_ml.api import init_app
from model import SamMobileSegmentation

app = init_app(SamMobileSegmentation)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090)
