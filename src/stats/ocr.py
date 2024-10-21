import logging

import cv2
import pytesseract

from definitions import RUNTIME_ENV

if RUNTIME_ENV == 'windows':
    pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files/Tesseract-OCR/tesseract.exe'
log = logging.getLogger(__name__)


# Tesseract Page segmentation modes (-- psm):
#   0    Orientation and script detection (OSD) only.
#   1    Automatic page segmentation with OSD.
#   2    Automatic page segmentation, but no OSD, or OCR.
#   3    Fully automatic page segmentation, but no OSD. (Default)
#   4    Assume a single column of text of variable sizes.
#   5    Assume a single uniform block of vertically aligned text.
#   6    Assume a single uniform block of text.
#   7    Treat the image as a single text line. <-- THIS
#   8    Treat the image as a single word.
#   9    Treat the image as a single word in a circle.
#  10    Treat the image as a single character.
#  11    Sparse text. Find as much text as possible in no particular order.
#  12    Sparse text with OSD.
#  13    Raw line. Treat the image as a single text line, bypassing hacks that are Tesseract-specific. <-- OR THIS
class OCR:
    @staticmethod
    def extract_text_from_image(img_path):
        try:
            img = cv2.imread(img_path)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            raw_text = pytesseract.image_to_string(gray, config=r'-l pol+eng --oem 3').replace("\n", " ")
        except Exception as e:
            log.info(f"OCR error on image {img_path}: {e}")
            raw_text = ''
        return raw_text
