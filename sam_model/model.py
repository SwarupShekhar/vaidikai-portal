import os
import io
import logging
import numpy as np
from PIL import Image
from label_studio_ml.model import LabelStudioMLBase

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", "/data/models")
SAM_CHOICE = os.getenv("SAM_CHOICE", "MobileSAM")


class SamMobileSegmentation(LabelStudioMLBase):
    """SAM-based segmentation ML backend for Label Studio."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = None
        self._load_model()

    def _load_model(self):
        try:
            if SAM_CHOICE == "MobileSAM":
                from mobile_sam import sam_model_registry, SamPredictor
                model_path = os.path.join(MODEL_DIR, "mobile_sam.pt")
                if not os.path.exists(model_path):
                    logger.warning(f"Model not found at {model_path}, downloading...")
                    import urllib.request
                    os.makedirs(MODEL_DIR, exist_ok=True)
                    urllib.request.urlretrieve(
                        "https://github.com/ChaoningZhang/MobileSAM/raw/master/weights/mobile_sam.pt",
                        model_path
                    )
                model_type = "vit_t"
                sam = sam_model_registry[model_type](checkpoint=model_path)
                sam.eval()
                self.model = SamPredictor(sam)
                logger.info(f"MobileSAM model loaded from {model_path}")
            else:
                logger.warning(f"Unknown SAM_CHOICE: {SAM_CHOICE}, skipping model load")
        except Exception as e:
            logger.error(f"Failed to load SAM model: {e}")
            self.model = None

    def predict(self, tasks, **kwargs):
        """Return predictions for the given tasks."""
        predictions = []
        for task in tasks:
            predictions.append({"result": [], "score": 0.0})
        return predictions

    def fit(self, completions, workdir=None, **kwargs):
        """Training is not supported for SAM."""
        return {"status": "Training not supported for SAM"}
