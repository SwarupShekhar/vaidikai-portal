import os
import cv2
import logging
import numpy as np

from label_studio_converter import brush
from typing import List, Dict, Optional
from uuid import uuid4
from sam_predictor import SAMPredictor
from label_studio_ml.model import LabelStudioMLBase

logger = logging.getLogger(__name__)

SAM_CHOICE = os.environ.get("SAM_CHOICE", "MobileSAM")  # other option is just SAM
PREDICTOR = SAMPredictor(SAM_CHOICE)


class SamMobileSegmentation(LabelStudioMLBase):
    """SAM-based segmentation ML backend for Label Studio."""

    def setup(self):
        self.set("model_version", f'{self.__class__.__name__}-v0.0.1')

    def predict(self, tasks: List[Dict], context: Optional[Dict] = None, **kwargs) -> List[Dict]:
        """ Returns the predicted mask for a smart keypoint that has been placed."""

        # Check if the project uses PolygonLabels or BrushLabels
        is_polygon = True
        try:
            from_name, to_name, value = self.get_first_tag_occurence('PolygonLabels', 'Image')
        except Exception:
            try:
                from_name, to_name, value = self.get_first_tag_occurence('BrushLabels', 'Image')
                is_polygon = False
            except Exception:
                logger.error("Could not find either PolygonLabels or BrushLabels in configuration.")
                return []

        if not context or not context.get('result'):
            # if there is no context, no interaction has happened yet (return empty predictions)
            return []

        image_width = context['result'][0]['original_width']
        image_height = context['result'][0]['original_height']

        # collect context information
        point_coords = []
        point_labels = []
        input_box = None
        selected_label = None
        for ctx in context['result']:
            x = ctx['value']['x'] * image_width / 100
            y = ctx['value']['y'] * image_height / 100
            ctx_type = ctx['type']
            selected_label = ctx['value'].get(ctx_type, [None])[0]
            if ctx_type == 'keypointlabels':
                point_labels.append(int(ctx.get('is_positive', 0)))
                point_coords.append([int(x), int(y)])
            elif ctx_type == 'rectanglelabels':
                box_width = ctx['value']['width'] * image_width / 100
                box_height = ctx['value']['height'] * image_height / 100
                input_box = [int(x), int(y), int(box_width + x), int(box_height + y)]

        logger.info(f'Point coords are {point_coords}, point labels are {point_labels}, input box is {input_box}')

        img_path = tasks[0]['data'][value]
        predictor_results = PREDICTOR.predict(
            img_path=img_path,
            point_coords=point_coords or None,
            point_labels=point_labels or None,
            input_box=input_box,
            task=tasks[0],
            path_resolver=self.get_local_path
        )

        predictions = self.get_results(
            masks=predictor_results['masks'],
            probs=predictor_results['probs'],
            width=image_width,
            height=image_height,
            from_name=from_name,
            to_name=to_name,
            label=selected_label,
            is_polygon=is_polygon
        )

        return predictions

    def get_results(self, masks, probs, width, height, from_name, to_name, label, is_polygon=False):
        results = []
        total_prob = 0
        for mask, prob in zip(masks, probs):
            label_id = str(uuid4())[:4]
            total_prob += prob

            if is_polygon:
                # Convert the binary mask to polygon contours
                binary_mask = (mask * 255).astype(np.uint8)
                contours, _ = cv2.findContours(binary_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                if not contours:
                    continue
                # Get largest contour
                contour = max(contours, key=cv2.contourArea)
                # Simplify the contour points to keep UI responsive
                epsilon = 0.003 * cv2.arcLength(contour, True)
                approx = cv2.approxPolyDP(contour, epsilon, True)
                
                points = []
                for pt in approx:
                    x, y = pt[0]
                    # Convert to percentage
                    x_pct = (x / width) * 100.0
                    y_pct = (y / height) * 100.0
                    points.append([x_pct, y_pct])

                results.append({
                    'id': label_id,
                    'from_name': from_name,
                    'to_name': to_name,
                    'original_width': width,
                    'original_height': height,
                    'image_rotation': 0,
                    'value': {
                        'points': points,
                        'polygonlabels': [label],
                    },
                    'score': prob,
                    'type': 'polygonlabels',
                    'readonly': False
                })
            else:
                # converting the mask from the model to RLE format which is usable in Label Studio
                mask = mask * 255
                rle = brush.mask2rle(mask)
                results.append({
                    'id': label_id,
                    'from_name': from_name,
                    'to_name': to_name,
                    'original_width': width,
                    'original_height': height,
                    'image_rotation': 0,
                    'value': {
                        'format': 'rle',
                        'rle': rle,
                        'brushlabels': [label],
                    },
                    'score': prob,
                    'type': 'brushlabels',
                    'readonly': False
                })

        return [{
            'result': results,
            'model_version': self.get('model_version'),
            'score': total_prob / max(len(results), 1)
        }]

    def fit(self, completions, workdir=None, **kwargs):
        """Training is not supported for SAM."""
        return {"status": "Training not supported for SAM"}
